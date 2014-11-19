package org.fastcatsearch.ir;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.fastcatsearch.datasource.reader.AbstractDataSourceReader;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.SettingException;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.document.DocumentWriter;
import org.fastcatsearch.ir.index.DeleteIdSet;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.ir.util.Formatter;
import org.fastcatsearch.job.state.IndexingTaskState;
import org.fastcatsearch.util.FilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCollectionDocumentStorer implements CollectionIndexerable {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractCollectionDocumentStorer.class);
	protected CollectionContext collectionContext;
	
	protected AbstractDataSourceReader dataSourceReader;
	protected long startTime;
	protected IndexingTaskState indexingTaskState;
	
	protected DeleteIdSet deleteIdSet; //삭제문서리스트. 외부에서 source reader를 통해 셋팅된다.
	
	protected DocumentWriter documentWriter;
	protected SegmentInfo workingSegmentInfo;
	protected int count;
	protected long lapTime;
	
	protected boolean stopRequested;
	
	public AbstractCollectionDocumentStorer(CollectionContext collectionContext) {
		this.collectionContext = collectionContext;
	}
	
	protected abstract AbstractDataSourceReader createDataSourceReader(File filePath, SchemaSetting schemaSetting) throws IRException;
	protected abstract void prepare() throws IRException;
	protected abstract boolean done(SegmentInfo segmentInfo, IndexStatus indexStatus) throws IRException;
	
	public void init(Schema schema) throws IRException {
		
		prepare();
		
		FilePaths dataFilePaths = collectionContext.collectionFilePaths().dataPaths();
		int dataSequence = collectionContext.getIndexSequence();

		IndexConfig indexConfig = collectionContext.indexConfig();
		
		logger.debug("WorkingSegmentInfo = {}", workingSegmentInfo);
		String segmentId = workingSegmentInfo.getId();
//		RevisionInfo revisionInfo = workingSegmentInfo.getRevisionInfo();

		File segmentDir = dataFilePaths.segmentFile(dataSequence, segmentId);
		logger.info("Segment Dir = {}", segmentDir.getAbsolutePath());
		
		
		File filePath = collectionContext.collectionFilePaths().file();
		dataSourceReader = createDataSourceReader(filePath, schema.schemaSetting());
		
		try{
			documentWriter = new DocumentWriter(schema.schemaSetting(), segmentDir);
		}catch(Exception e){
			throw new IRException(e);
		}
		
		startTime = System.currentTimeMillis();
	}

	public void addDocument(Document document) throws IRException, IOException{
		documentWriter.write(document);
		count++;
		if (count % 10000 == 0) {
			logger.info(
					"{} documents stored, lap = {} ms, elapsed = {}, mem = {}",
					count, System.currentTimeMillis() - lapTime,
							Formatter.getFormatTime(System.currentTimeMillis() - startTime),
							Formatter.getFormatSize(Runtime.getRuntime().totalMemory()));
			lapTime = System.currentTimeMillis();
		}
		if(indexingTaskState != null){
			indexingTaskState.incrementDocumentCount();
		}
	}
	@Override
	public void requestStop(){
		logger.info("Collection [{}] Document Store Stop Requested! ", collectionContext.collectionId());
		
		stopRequested = true;
	}
	
	//색인취소(0건)이면 false;
	@Override
	public boolean close() throws IRException, SettingException{
		
//		RevisionInfo revisionInfo = workingSegmentInfo.getRevisionInfo();
		if (documentWriter != null) {
			try {
				documentWriter.close();
//				documentWriter.getIndexWriteInfo(indexWriteInfoList);
			} catch (IOException e) {
				throw new IRException(e);
			}
		}

		dataSourceReader.close();
		
		logger.debug("##Indexer close {}", workingSegmentInfo);
		deleteIdSet = dataSourceReader.getDeleteList();
		int deleteCount = deleteIdSet.size();
		workingSegmentInfo.setDocumentCount(count);
		workingSegmentInfo.setInsertCount(count);
		workingSegmentInfo.setDeleteCount(deleteCount);
		workingSegmentInfo.setCreateTime(Formatter.formatDate());
		
		long endTime = System.currentTimeMillis();
		
		IndexStatus indexStatus = new IndexStatus(workingSegmentInfo.getDocumentCount(), workingSegmentInfo.getInsertCount(), workingSegmentInfo.getUpdateCount(), deleteCount,
				Formatter.formatDate(new Date(startTime)), Formatter.formatDate(new Date(endTime)), Formatter.getFormatTime(endTime - startTime));
		
		logger.debug("CLOSE >> indexStatus > {}", indexStatus);
		done(workingSegmentInfo, indexStatus);
		return true;
	}
	
	@Override
	public void doIndexing() throws IRException, IOException {
		lapTime = System.currentTimeMillis();
		while (dataSourceReader.hasNext()) {
			if(stopRequested){
				break;
			}
			Document document = dataSourceReader.nextDocument();
//			logger.debug("doc >> {}", document);
			addDocument(document);
		}

	}
	
	public DeleteIdSet deleteIdSet() {
		return deleteIdSet;
	}
	
	public void setState(IndexingTaskState indexingTaskState) {
		this.indexingTaskState = indexingTaskState;
	}
}
