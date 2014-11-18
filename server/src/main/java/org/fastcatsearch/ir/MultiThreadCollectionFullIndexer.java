package org.fastcatsearch.ir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.datasource.reader.DataSourceReader;
import org.fastcatsearch.datasource.reader.DefaultDataSourceReaderFactory;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.common.SettingException;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.DataSourceConfig;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.index.DeleteIdSet;
import org.fastcatsearch.ir.index.IndexWritable;
import org.fastcatsearch.ir.index.IndexWriteInfoList;
import org.fastcatsearch.ir.index.SegmentIndexWriteConsumer;
import org.fastcatsearch.ir.index.SegmentWriter;
import org.fastcatsearch.ir.index.SelectedIndexList;
import org.fastcatsearch.ir.index.WriteInfoLoggable;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.ir.util.Formatter;
import org.fastcatsearch.job.indexing.IndexingStopException;
import org.fastcatsearch.job.state.IndexingTaskState;
import org.fastcatsearch.util.CollectionContextUtil;
import org.fastcatsearch.util.FilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadCollectionFullIndexer implements CollectionIndexerable {
	protected static final Logger logger = LoggerFactory.getLogger(MultiThreadCollectionFullIndexer.class);
	protected CollectionContext collectionContext;
	protected AnalyzerPoolManager analyzerPoolManager;
	
	protected DataSourceReader dataSourceReader;
	protected long startTime;
	protected IndexingTaskState indexingTaskState;
	
	protected DeleteIdSet deleteIdSet; //삭제문서리스트. 외부에서 source reader를 통해 셋팅된다.
	
	protected IndexWriteInfoList indexWriteInfoList;
	
	private SegmentInfo totalSegmentInfo; //하위 세그먼트들의 값을 모두 합쳐서 가지고 있게 된다.
	protected List<SegmentInfo> workingSegmentInfoList;
	
	protected int count;
	protected long lapTime;
	
	protected boolean stopRequested;
	protected SelectedIndexList selectedIndexList;// 색인필드 선택사항.
	protected int segmentSize; //동시에 분할 생성할 segment 갯수. 
	private BlockingQueue<Document> documentQueue;
	private CountDownLatch latch;
	private List<SegmentIndexWriteConsumer> consumerList;
	
	
	public MultiThreadCollectionFullIndexer(SegmentInfo segmentInfo, List<SegmentInfo> segmentInfoList, CollectionContext collectionContext, AnalyzerPoolManager analyzerPoolManager) throws IRException {
		this(segmentInfo, segmentInfoList, collectionContext, analyzerPoolManager, null);
	}
	public MultiThreadCollectionFullIndexer(SegmentInfo segmentInfo, List<SegmentInfo> segmentInfoList, CollectionContext collectionContext, AnalyzerPoolManager analyzerPoolManager, SelectedIndexList selectedIndexList) throws IRException {
		this.totalSegmentInfo = segmentInfo;
		this.workingSegmentInfoList = segmentInfoList;
		this.collectionContext = collectionContext;
		this.analyzerPoolManager = analyzerPoolManager;
		this.selectedIndexList = selectedIndexList;
		this.segmentSize = collectionContext.collectionConfig().getFullIndexingSegmentSize();
		init(collectionContext.schema());
	}

	private DataSourceReader createDataSourceReader(File filePath, SchemaSetting schemaSetting) throws IRException{
		DataSourceConfig dataSourceConfig = collectionContext.dataSourceConfig();
		return DefaultDataSourceReaderFactory.createFullIndexingSourceReader(filePath, schemaSetting, dataSourceConfig);
	}
	
	private boolean done(IndexStatus indexStatus) throws IRException, IndexingStopException {
		int insertCount = totalSegmentInfo.getInsertCount();

		if (insertCount > 0 && !stopRequested) {
			//이미 동일한 revinfo이므로 재셋팅필요없다.
			//workingSegmentInfo.updateRevision(revisionInfo);
			
			int baseNumber = 0;
			SegmentInfo workingSegmentInfo = null;
			for (int inx = 0; inx < segmentSize; inx++) {
				workingSegmentInfo = workingSegmentInfoList.get(inx);
				//문서수가 스레드 수보다 작은경우 발생할 수 있는 오류 제어.
				if(workingSegmentInfo.getDocumentCount() == 0) {
					break;
				}
				workingSegmentInfo.setBaseNumber(baseNumber);
				//update index#/info.xml file
				//addindexing의 updateCollection대신 호출.
				collectionContext.addSegmentInfo(workingSegmentInfo);
				logger.debug("Add Segment info = {}", workingSegmentInfo);
				baseNumber = workingSegmentInfo.getNextBaseNumber();
			}
			//update status.xml file
			collectionContext.updateCollectionStatus(IndexingType.FULL, totalSegmentInfo, startTime, System.currentTimeMillis());
			collectionContext.indexStatus().setFullIndexStatus(indexStatus);
			
			return true;
		}else{
			if(!stopRequested){
				logger.info("[{}] Indexing Canceled due to no documents.", collectionContext.collectionId());
				throw new IndexingStopException(collectionContext.collectionId()+" Indexing Canceled due to no documents.");
			}else{
				logger.info("[{}] Indexing Canceled due to Stop Requested!", collectionContext.collectionId());
				throw new IndexingStopException(collectionContext.collectionId()+" Indexing Canceled due to Stop Requested");
			}
		}
		
	}
	
	protected IndexWritable createIndexWriter(Schema schema, File segmentDir, SegmentInfo segmentInfo, IndexConfig indexConfig) throws IRException {
		return new SegmentWriter(schema, segmentDir, segmentInfo, indexConfig, analyzerPoolManager, selectedIndexList);
	}
	
	public void init(Schema schema) throws IRException {

		prepare();
		
		documentQueue = new LinkedBlockingQueue<Document>(10);
		latch = new CountDownLatch(segmentSize);
		
		FilePaths dataFilePaths = collectionContext.collectionFilePaths().dataPaths();
		int dataSequence = collectionContext.getIndexSequence();
		consumerList = new ArrayList<SegmentIndexWriteConsumer>();
		IndexConfig indexConfig = collectionContext.indexConfig();
		for (int inx = 0; inx < segmentSize; inx++) {
			SegmentInfo workingSegmentInfo = workingSegmentInfoList.get(inx);
			logger.debug("WorkingSegmentInfo-{} = {}", inx, workingSegmentInfo);
			
			String segmentId = workingSegmentInfo.getId();
//			RevisionInfo revisionInfo = workingSegmentInfo.getRevisionInfo();
			
			File segmentDir = dataFilePaths.segmentFile(dataSequence, segmentId);
			logger.info("Segment Dir = {}", segmentDir.getAbsolutePath());
			IndexWritable indexWriter = createIndexWriter(schema, segmentDir, workingSegmentInfo, indexConfig);
			consumerList.add(new SegmentIndexWriteConsumer(segmentId, indexWriter, documentQueue, latch));
		}
		File filePath = collectionContext.collectionFilePaths().file();
		dataSourceReader = createDataSourceReader(filePath, schema.schemaSetting());
		
		indexWriteInfoList = new IndexWriteInfoList();
		
		startTime = System.currentTimeMillis();
	}

	protected void prepare() throws IRException {
//		workingSegmentInfoList = new ArrayList<SegmentInfo>(segmentSize);
//		SegmentInfo workingSegmentInfo = new SegmentInfo();
//		workingSegmentInfoList.add(workingSegmentInfo);
//		//순차적 세그먼트 info 를 할당한다.
//		for (int inx = 1; inx < segmentSize; inx++) {
//			workingSegmentInfo = workingSegmentInfo.getNextSegmentInfo();
//			workingSegmentInfoList.add(workingSegmentInfo);
//		}
		
		// data 디렉토리를 변경한다.
		int newDataSequence = collectionContext.nextDataSequence();

		// 디렉토리 초기화.
		File indexDataDir = collectionContext.collectionFilePaths().dataPaths().indexDirFile(newDataSequence);
		try {
			//FileUtils.deleteDirectory(indexDataDir);
			FileUtils.forceDelete(indexDataDir);
		} catch (IOException e) {
			throw new IRException(e);
		}

		collectionContext.clearDataInfoAndStatus();
		indexDataDir.mkdirs();
	}

	@Override
	public void requestStop(){
		logger.info("Collection [{}] Indexer Stop Requested! ", collectionContext.collectionId());
		
		stopRequested = true;
	}
	
	//색인취소(0건)이면 false;
	@Override
	public boolean close() throws IRException, SettingException, IndexingStopException {
//		RevisionInfo revisionInfo = new RevisionInfo();
		
		for (int inx = 0; inx < segmentSize; inx++) {
			
			try {
				IndexWritable indexWriter = consumerList.get(inx).getWriter();
				indexWriter.close();
				if(indexWriter instanceof WriteInfoLoggable)
				((WriteInfoLoggable) indexWriter).getIndexWriteInfo(indexWriteInfoList);
			} catch (IOException e) {
				throw new IRException(e);
			}
			
			SegmentInfo subSegmentInfo = workingSegmentInfoList.get(inx);
			totalSegmentInfo.setDocumentCount(totalSegmentInfo.getDocumentCount() + subSegmentInfo.getDocumentCount());
			totalSegmentInfo.setInsertCount(totalSegmentInfo.getInsertCount() + subSegmentInfo.getInsertCount());
			totalSegmentInfo.setUpdateCount(totalSegmentInfo.getUpdateCount() + subSegmentInfo.getUpdateCount());
//			revisionInfo.add(subRevisionInfo);
//			logger.debug("revisionInfo#{} > {}", inx, revisionInfo);
		}

		dataSourceReader.close();
		
//		logger.debug("##Indexer close {}", revisionInfo);
		deleteIdSet = dataSourceReader.getDeleteList();
		int deleteCount = 0;
		if(deleteIdSet != null) {
			deleteCount = deleteIdSet.size();
		}
		
		totalSegmentInfo.setDeleteCount(deleteCount);
		
		long endTime = System.currentTimeMillis();
		
		IndexStatus indexStatus = new IndexStatus(totalSegmentInfo.getDocumentCount(), totalSegmentInfo.getInsertCount(), totalSegmentInfo.getUpdateCount(), deleteCount,
				Formatter.formatDate(new Date(startTime)), Formatter.formatDate(new Date(endTime)), Formatter.getFormatTime(endTime - startTime));
		
		if(done(indexStatus)){
			CollectionContextUtil.saveCollectionAfterIndexing(collectionContext);
		}else{
			//저장하지 않음.
		}
		return true;
	}
	
	public IndexWriteInfoList indexWriteInfoList() {
		return indexWriteInfoList;
	}
	
	@Override
	public void doIndexing() throws IRException, IOException {
		
		indexingTaskState.setStep(IndexingTaskState.STEP_INDEXING);
		for (SegmentIndexWriteConsumer consumer : consumerList) {
			consumer.start();
		}
		try {
			lapTime = System.currentTimeMillis();
			while (dataSourceReader.hasNext()) {
				if (stopRequested) {
					break;
				}
				Document document = dataSourceReader.nextDocument();
				documentQueue.put(document);
				count++;
				if (count % 10000 == 0) {
					logger.info("{} documents indexed, lap = {} ms, elapsed = {}, mem = {}", count, System.currentTimeMillis() - lapTime,
							Formatter.getFormatTime(System.currentTimeMillis() - startTime), Formatter.getFormatSize(Runtime.getRuntime().totalMemory()));
					lapTime = System.currentTimeMillis();
				}
				if (indexingTaskState != null) {
					indexingTaskState.incrementDocumentCount();
				}
			}

			for (SegmentIndexWriteConsumer consumer : consumerList) {
				consumer.requestDone();
			}
			
			latch.await();

		} catch (Exception e) {
			throw new IRException(e);
		}
	}
	
	public DeleteIdSet deleteIdSet() {
		return deleteIdSet;
	}
	
	public void setTaskState(IndexingTaskState indexingTaskState) {
		this.indexingTaskState = indexingTaskState;
	}
}