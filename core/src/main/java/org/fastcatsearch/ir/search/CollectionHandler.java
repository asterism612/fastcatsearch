package org.fastcatsearch.ir.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.fastcatsearch.ir.analysis.AnalyzerFactoryManager;
import org.fastcatsearch.ir.analysis.AnalyzerPool;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.common.SettingException;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.DataPlanConfig;
import org.fastcatsearch.ir.document.PrimaryKeyIndexBulkReader;
import org.fastcatsearch.ir.document.PrimaryKeyIndexReader;
import org.fastcatsearch.ir.document.merge.PrimaryKeyIndexMerger;
import org.fastcatsearch.ir.index.DeleteIdSet;
import org.fastcatsearch.ir.index.PrimaryKeys;
import org.fastcatsearch.ir.io.BitSet;
import org.fastcatsearch.ir.io.BytesBuffer;
import org.fastcatsearch.ir.settings.AnalyzerSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.util.Counter;
import org.fastcatsearch.ir.util.DummyCounter;
import org.fastcatsearch.util.FilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionHandler {
	private static Logger logger = LoggerFactory.getLogger(CollectionHandler.class);
	private String collectionId;
	private CollectionContext collectionContext;
	private CollectionSearcher collectionSearcher;
	private List<SegmentReader> segmentReaderList;
	private Schema schema;
	private long startedTime;
	private boolean isLoaded;
	private FilePaths collectionFilePaths;

	private AnalyzerFactoryManager analyzerFactoryManager;
	private AnalyzerPoolManager analyzerPoolManager;

	private Counter queryCounter;

	public CollectionHandler(CollectionContext collectionContext, AnalyzerFactoryManager analyzerFactoryManager) throws IRException, SettingException {
		this.collectionContext = collectionContext;
		this.collectionId = collectionContext.collectionId();
		this.collectionFilePaths = collectionContext.collectionFilePaths();
		this.analyzerFactoryManager = analyzerFactoryManager;

		queryCounter = new DummyCounter();
	}

	public CollectionHandler load() throws IRException {
		loadSearcherAndReader();
		this.collectionSearcher = new CollectionSearcher(this);
		startedTime = System.currentTimeMillis();
		isLoaded = true;
		logger.info("Collection[{}] Loaded! {}", collectionId, collectionFilePaths.file().getAbsolutePath());
		return this;
	}

	@Deprecated
	public void setAnalyzerPoolManager(AnalyzerPoolManager analyzerPoolManager) {
		this.analyzerPoolManager = analyzerPoolManager;
	}

	public AnalyzerPoolManager analyzerPoolManager() {
		return analyzerPoolManager;
	}

	public Schema schema() {
		return schema;
	}

	public long getStartedTime() {
		return startedTime;
	}

	public FilePaths indexFilePaths() {
		return collectionFilePaths;
	}

	public CollectionContext collectionContext() {
		return collectionContext;
	}

	public CollectionSearcher searcher() {
		return collectionSearcher;
	}

	public boolean isLoaded() {
		return isLoaded;
	}

	private void loadSearcherAndReader() throws IRException {

		analyzerPoolManager = new AnalyzerPoolManager();
		List<AnalyzerSetting> analyzerSettingList = collectionContext.schema().schemaSetting().getAnalyzerSettingList();
		analyzerPoolManager.register(analyzerSettingList, analyzerFactoryManager);

		this.schema = collectionContext.schema();
		int dataSequence = collectionContext.indexStatus().getSequence();
		FilePaths dataPaths = collectionFilePaths.dataPaths();
		File dataDir = dataPaths.indexFilePaths(dataSequence).file();
		if (!dataDir.exists()) {
			logger.info("create collection data directory [{}]", dataDir.getAbsolutePath());
			dataDir.mkdir();
		}

		logger.debug("Load CollectionHandler [{}] data >> {}", collectionId, dataDir.getAbsolutePath());

		// 색인기록이 있다면 세그먼트를 로딩한다.
		segmentReaderList = new ArrayList<SegmentReader>();
		List<SegmentInfo> segmentInfoList = collectionContext.dataInfo().getSegmentInfoList();
		int segmentSize = segmentInfoList.size();

		// logger.debug("segmentInfoList > {}, {}", segmentInfoList.size(), segmentInfoList);
		if (segmentSize > 0) {
			// 0,1,2...차례대로 list에 존재해야한다. deleteset을 적용해야하기때문에..
			SegmentInfo lastSegmentInfo = segmentInfoList.get(segmentSize - 1);
//			File lastRevisionDir = dataPaths.revisionFile(dataSequence, lastSegmentInfo.getId(), lastSegmentInfo.getRevision());
			File lastSegmentDir = dataPaths.segmentFile(dataSequence, lastSegmentInfo.getId());
			try {
				for (SegmentInfo segmentInfo : collectionContext.dataInfo().getSegmentInfoList()) {
					File segmentDir = dataPaths.segmentFile(dataSequence, segmentInfo.getId());
					// 삭제문서는 마지막 세그먼트의 마지막 리비전에 최신 업데이트 파일이 있으므로, 그것을 로딩한다.
					BitSet deleteSet = new BitSet(lastSegmentDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentInfo.getId()));
					segmentReaderList.add(new SegmentReader(segmentInfo, schema, segmentDir, deleteSet, analyzerPoolManager));
					logger.debug("{}", segmentInfo);
				}
			} catch (IOException e) {
				throw new IRException(e);
			}
		}
	}

	public void close() throws IOException {
		logger.info("Close Collection handler {}", collectionId);
		if (segmentReaderList != null) {
			for (SegmentReader segmentReader : segmentReaderList) {
				segmentReader.close();
			}
		}
		collectionSearcher = null;
		isLoaded = false;
	}

	public String collectionId() {
		return collectionId;
	}

	public int getDataSequence() {
		return collectionContext.indexStatus().getSequence();

	}

	public void printSegmentStatus() {
		int i = 0;
		for (SegmentReader segmentReader : segmentReaderList) {
			logger.info("SEG#{} >> {}", i++, segmentReader.segmentInfo());
		}
	}

	public SegmentReader segmentReader(int segmentNumber) {
		if (segmentReaderList.size() == 0) {
			return null;
		}

		return segmentReaderList.get(segmentNumber);

	}

	public SegmentReader getLastSegmentReader() {
		if (segmentReaderList.size() == 0) {
			return null;
		}

		// get Last segment Number
		return segmentReaderList.get(segmentReaderList.size() - 1);
	}

	public SegmentSearcher segmentSearcher(int segmentNumber) {
		if (segmentReaderList.size() == 0) {
			return null;
		}

		return segmentReaderList.get(segmentNumber).segmentSearcher();
	}

	// public Schema schema() {
	// return collectionContext.schema();
	// }

	// segment reader 추가.
	// collectionContext에는 segmentInfo를 추가하지 않는다.
	// 색인이 끝나면서 이미 context에 segmentinfo가 추가되어있는 상태이다.
	private void addSegmentReader(SegmentReader segmentReader) {
		segmentReaderList.add(segmentReader);
		// info.xml 파일업데이트용.
		collectionContext.updateSegmentInfo(segmentReader.segmentInfo());
	}

	// segment reader 교체.
	private void updateSegmentReader(SegmentReader segmentReader, SegmentReader prevSegmentReader) {
		segmentReaderList.remove(prevSegmentReader);
		segmentReaderList.add(segmentReader);
		// info.xml 파일업데이트용.
		collectionContext.updateSegmentInfo(segmentReader.segmentInfo());
		
		/*
		 * 불필요한 revision 디렉토리 삭제.
		 * */
		DataPlanConfig dataPlanConfig = collectionContext.collectionConfig().getDataPlanConfig();
//		int revisionBackupSize = dataPlanConfig.getSegmentRevisionBackupSize();
//		if(revisionBackupSize > 0) {
//			//삭제가 필요한 revision은 삭제한다.
//			SegmentInfo segmentInfo = segmentReader.segmentInfo();
////			RevisionInfo revisionInfo = segmentInfo.getRevisionInfo();
//			int revisionId = revisionInfo.getId();
//			//TODO 일단 REF는 무시하지만, 추가문서없이 delete만 계속 될경우 원본이 삭제될수 있는 잠재적 버그존재.
////			int revisionRefId = revisionInfo.getRef();
//			
//			//targetRevisionId는 삭제할 id 
//			int targetRevisionId = revisionId - revisionBackupSize - 1;
//			if(targetRevisionId >= 0){
//				File segmentDir = segmentReader.segmentDir();
//				File deleteRevisionDir = new File(segmentDir, String.valueOf(targetRevisionId));
//				try {
//					//FileUtils.deleteDirectory(deleteRevisionDir);
//					FileUtils.forceDelete(deleteRevisionDir);
//					logger.debug("Delete backup revision directory = {}", deleteRevisionDir.getAbsolutePath());
//				} catch (IOException e) {
//					logger.error("Error while delete backup revision directory = " + deleteRevisionDir.getAbsolutePath(), e);
//				}
//			}
//		
//		}
		
	}

	// SegmentReader 찾기.
	private SegmentReader getSegmentReader(String segmentId) {
		for (SegmentReader segmentReader : segmentReaderList) {
			if (segmentReader.segmentInfo().getId().equals(segmentId)) {
				return segmentReader;
			}
		}
		return null;
	}

	/**
	 * 증분색인후 호출하는 메소드이다. 1. 이전 세그먼트에 삭제 문서를 적용한다. 2. 해당 segment reader 재로딩 및 대체하기.
	 * */
	public void updateCollection(CollectionContext collectionContext, SegmentInfo segmentInfo, File segmentDir) throws IOException, IRException {

		this.collectionContext = collectionContext;
		//2014-11-17 swsong :증분색인시 무조건 segment가 추가된다.
		
		//색인시 삭제요청 들어온 문서아이디 리스트.
		DeleteIdSet deleteIdSet = null;
		File deleteListFile = new File(segmentDir, IndexFileNames.docDeleteList);
		if(deleteListFile.exists() && deleteListFile.length() > 0) {
			BufferedReader reader = null;
			try{
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(deleteListFile)));
				String keyString = reader.readLine();
				String[] keys = keyString.split("\t");
				deleteIdSet = new DeleteIdSet(-1);
				deleteIdSet.add(new PrimaryKeys(keys));
			}finally{
				if(reader != null) {
					reader.close();
				}
			}
		}
		
		BitSet[] deleteSetList = new BitSet[segmentReaderList.size()];
		String segmentId = segmentInfo.getId();
		for (SegmentReader segmentReader : segmentReaderList) {
			File prevSegmentDir = segmentReader.segmentDir();
			// 복사해온 delete.set.#들을 기존 pk들을 확인하면서 update해준다. 파일에 write까지 수행됨.
			// 예를들어 현재 segment가 5이면 수정파일은 delete.set.0,1,2,3,4 이다.
			
			updateDeleteSetWithSegments(segmentId, prevSegmentDir, deleteIdSet, deleteSetList);
		}
		//적용
		for (int i = 0; i < segmentReaderList.size(); i++) {
			segmentReaderList.get(i).setDeleteSet(deleteSetList[i]);
		}
		// 새로생성된 세그먼트는 로딩하여 리스트에 추가해준다.
		addSegmentReader(new SegmentReader(segmentInfo, schema, segmentDir, analyzerPoolManager));
	}
	/*
	 * Indexing작업으로 생성된 pk와 delete list 를 이전 segment들과 비교해보면서 세그먼트별 delete.set.#들을 업데이트한다. delete.set.# 파일들은 최종 revision디렉토리안에 존재한다.
	 */
	private int[] updateDeleteSetWithSegments(String segmentId, File segmentDir, DeleteIdSet deleteIdSet, BitSet[] prevDeleteSetList) throws IOException {
		int[] updateAndDelete = new int[] { 0, 0 };
		// 첨자 i는 세그먼터 id와 일치해야한다.
		int prevSegmentSize = segmentReaderList.size();
		PrimaryKeyIndexReader[] prevPkReaderList = new PrimaryKeyIndexReader[prevSegmentSize];

		for (int i = 0; i < prevSegmentSize; i++) {
			SegmentReader prevSegmentReader = segmentReaderList.get(i);
			File prevSegmentDir = prevSegmentReader.segmentDir();
			String id = prevSegmentReader.segmentInfo().getId();
			
			String filename = IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, id);
			File f1 = new File(prevSegmentDir, filename);
			File f2 = new File(prevSegmentDir, filename +".tmp");
			FileUtils.copyFile(f1, f2);
			
			prevPkReaderList[i] = new PrimaryKeyIndexReader(prevSegmentDir, IndexFileNames.primaryKeyMap);
			prevDeleteSetList[i] = new BitSet(f2);
		}
		
		File pkFile = new File(segmentDir, IndexFileNames.primaryKeyMap);
		File pkIndexFile = new File(segmentDir, IndexFileNames.getIndexFileName(IndexFileNames.primaryKeyMap));
		File delFile = new File(segmentDir, IndexFileNames.docDeleteSet);
		updateAndDelete = applyUpdateAndDeleteToPrevSegments(pkFile, pkIndexFile, delFile, deleteIdSet, prevPkReaderList, prevDeleteSetList);

		for (int i = 0; i < prevSegmentSize; i++) {
			prevPkReaderList[i].close();
			prevDeleteSetList[i].save();
			logger.debug("New delete.set saved. set={}", prevDeleteSetList[i]);
		}

		/*
		 * delete.tmp를 delete파일로 바꾸고 로딩해준다.
		 * */
		for (int i = 0; i < prevSegmentSize; i++) {
			SegmentReader prevSegmentReader = segmentReaderList.get(i);
			File prevSegmentDir = prevSegmentReader.segmentDir();
			String id = prevSegmentReader.segmentInfo().getId();
			String filename = IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, id);
			File f1 = new File(prevSegmentDir, filename);
			File f2 = new File(prevSegmentDir, filename +".tmp");
			FileUtils.copyFile(f2, f1);
			FileUtils.deleteQuietly(f2);
			prevDeleteSetList[i] = new BitSet(f1);
		}
		return updateAndDelete;
	}
	
	/*
	 * 이번에 색인된 pkFile을 이전 pkFile들과 비교하면서 중복된 pk는 deleteSetList에 추가해준다. deleteSet에 추가된 갯수는 업데이트문서 갯수를 의미하며, 해당갯수는 결과로 리턴한다.
	 */
	private int[] applyUpdateAndDeleteToPrevSegments(File pkFile, File pkIndexFile, File delFile, DeleteIdSet deleteIdSet, PrimaryKeyIndexReader[] prevPkReaderList, BitSet[] prevDeleteSetList) throws IOException {

		// 이전 모든 세그먼트를 통틀어 업데이트되고 삭제된 문서수.
		int updateDocumentSize = 0; // 이번 pk와 이전 pk가 동일할 경우
		int deleteDocumentSize = 0;
		
		
		/**
		 * 1. 추가할 segment의 pk를 이전 segment의 pk와 비교하여 동일한것이 발견되면, 이전 segment의 문서를 지운다. 삭제마킹.
		 * */
		// 현 pk를 bulk로 읽어들여 id 중복을 확인한다.
		PrimaryKeyIndexBulkReader pkBulkReader = null;

		try { 
			pkBulkReader = new PrimaryKeyIndexBulkReader(pkFile);
			// 제약조건: pk 크기는 1k를 넘지않는다.
			BytesBuffer buf = new BytesBuffer(1024);
			// 새로 추가된 pk가 이전 세그먼트에 존재하면 update된 것이다.
			while (pkBulkReader.next(buf) != -1) {
				// backward matching
				boolean updated = false;
				for (int i = prevPkReaderList.length - 1; i >= 0; i--) {
					int localDocNo = prevPkReaderList[i].get(buf);
					// logger.debug("check "+new String(buf.array, 0, buf.limit));
					if (localDocNo != -1) {
						if (!prevDeleteSetList[i].isSet(localDocNo)) {
							// add delete list
							prevDeleteSetList[i].set(localDocNo);
						}
						if(!updated) {
							//이번문서는 이전 문서를 지우고 업데이트를 수행한것임을 알려준다.
							//한번만 수행하면 되므로, updated가 아닐경우만 셋팅해준다.
							updated = true;
							updateDocumentSize++;// updateSize 증가
						}
						// if found later pkmap, earlier pkmap already deleted the same pk.
	//					break;
						// 2014-11-17 swsong: 모든 이전 segment에 접근해서 지워준다. 
					}
				}
	
				buf.clear();
			}
		}finally {
			pkBulkReader.close();
		}
		
		/**
		 * 삭제요청들어온 문서 id들을 처리한다.
		 * */
		if(deleteIdSet != null) {
			PrimaryKeyIndexReader currentPkReader = null;
			try{
				BitSet deleteSet = new BitSet(delFile);
				currentPkReader = new PrimaryKeyIndexReader(pkFile, pkIndexFile);
				PrimaryKeysToBytesRef primaryKeysToBytesRef = new PrimaryKeysToBytesRef(schema);
				Iterator<PrimaryKeys> iterator = deleteIdSet.iterator();
				while (iterator.hasNext()) {
					PrimaryKeys ids = iterator.next();
					logger.debug("--- delete id = {}", ids);
		
					BytesRef bytesRef = primaryKeysToBytesRef.getBytesRef(ids);
					
					int localDocNo = currentPkReader.get(bytesRef);
					if (localDocNo != -1) {
						// add delete list
						logger.debug("--- delete localDocNo = {} at current segment", localDocNo);
						deleteSet.set(localDocNo);
						deleteDocumentSize++;// deleteSize 증가.
						// 현재 세그먼트에서 삭제된 pk는, 이전 세그먼트에서 찾아볼필요가 없다.
						// 왜냐하면 현재세그먼트와 동일한 pk가 이전 세그먼트에 존재한다면 해당 pk는 이미 삭제리스트에 존재할것이기 때문이다.
						// 저위의 1번에서 이미 처리됨.
						continue;
					}
					// backward matching
					for (int i = prevPkReaderList.length - 1; i >= 0; i--) {
						localDocNo = prevPkReaderList[i].get(bytesRef);
						if (localDocNo != -1) {
							if (!prevDeleteSetList[i].isSet(localDocNo)) {
								// add delete list
								logger.debug("--- delete doc[{}] at segment[{}]", localDocNo, i);
								prevDeleteSetList[i].set(localDocNo);
								//2014-11-17 swsong: 다른 세그먼트의 삭제 갯수는 여기에 기록하지 않는다.
	//							deleteDocumentSize++;// deleteSize 증가.
							}
							// 2014-11-17 swsong: 모든 이전 segment에 접근해서 지워준다.
//							break;
						}
					}
				}
			}finally {
				if(currentPkReader != null) {
					currentPkReader.close();
				}
			}
		}
		
		return new int[] { updateDocumentSize, deleteDocumentSize };
	}
	
	// 이전 세그먼트가 존재하면 delete.set을 업데이트하여 segment reader 에 적용시켜준다.
	// 최근 색인작업으로 추가된 newSegmentInfo 세그먼트의 문서는 최신이므로 delete.set을 따로 적용할 필요가 없다.
//	private int[] makeDeleteSetWithSegments(SegmentInfo segmentInfo, File segmentDir, List<SegmentReader> prevSegmentReaderList, DeleteIdSet deleteSet, BitSet[] deleteSetList)
//			throws IOException {
//		String segmentId = segmentInfo.getId();
//		int prevSegmentSize = prevSegmentReaderList.size();
//
////		File targetRevisionDir = new File(segmentDir, segmentInfo.getRevisionName());
//
//		if (prevSegmentSize > 0) {
//
//			SegmentReader lastSegmentReader = prevSegmentReaderList.get(prevSegmentSize - 1);
//			File lastSegmentDir = lastSegmentReader.segmentDir();//revisionDir();
//
//			// 이전 revision에서 delete.set.#들을 복사해온다.
//			copyDeleteSet(prevSegmentReaderList, lastSegmentDir, segmentDir);
//		}
//
//		// 복사해온 delete.set.#들을 기존 pk들을 확인하면서 update해준다. 파일에 write까지 수행됨.
//		// 예를들어 현재 segment가 5이면 수정파일은 delete.set.0,1,2,3,4 이다.
//		return updateDeleteSetWithSegments(segmentId, segmentDir, deleteSet, prevSegmentReaderList, deleteSetList);
//	}

	

	// 색인되어있는 세그먼트를 단순히 추가만한다. delete.set파일은 이미 수정되어있다고 가정한다.
//	public void addSegmentApplyCollection(SegmentInfo segmentInfo, File segmentDir) throws IOException, IRException {
////		File lastRevisionDir = new File(segmentDir, segmentInfo.getRevisionName());
//		// 삭제문서는 마지막 세그먼트의 마지막 리비전에 최신 업데이트 파일이 있으므로, 그것을 로딩한다.
//		for (int i = 0; i < segmentReaderList.size(); i++) {
//			SegmentInfo prevSegmentInfo = segmentReaderList.get(i).segmentInfo();
//			BitSet deleteSet = new BitSet(segmentDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, prevSegmentInfo.getId()));
//			segmentReaderList.get(i).setDeleteSet(deleteSet);
//		}
//		addSegmentReader(new SegmentReader(segmentInfo, schema, segmentDir, analyzerPoolManager));
//	}

	// 단순 update. delete.set파일은 이미 수정되어있다고 가정한다.
//	public void updateSegmentApplyCollection(SegmentInfo segmentInfo, File segmentDir) throws IOException, IRException {
//		if (segmentReaderList.size() == 0) {
//			return;
//		}
////		File lastRevisionDir = new File(segmentDir, segmentInfo.getRevisionName());
//		String segmentId = segmentInfo.getId();
//		SegmentReader oldSegmentReader = getSegmentReader(segmentId);
//		logger.debug("updateSegmentApplyShard segId={}, reader={}, size={}", segmentId, oldSegmentReader, segmentReaderList.size());
//		List<SegmentReader> prevSegmentReaderList = segmentReaderList.subList(0, segmentReaderList.size() - 1);
//		for (int i = 0; i < prevSegmentReaderList.size(); i++) {
//			SegmentInfo prevSegmentInfo = prevSegmentReaderList.get(i).segmentInfo();
//			BitSet deleteSet = new BitSet(segmentDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, prevSegmentInfo.getId()));
//			prevSegmentReaderList.get(i).setDeleteSet(deleteSet);
//		}
//		// 새 revison을 읽는 segmentReader를 만들어서 기존것과 바꾼다.
//		updateSegmentReader(new SegmentReader(segmentInfo, schema, segmentDir, analyzerPoolManager), oldSegmentReader);
//		// 기존 reader는 닫는다.
//		oldSegmentReader.close();
//	}



	private void copyDeleteSet(List<SegmentReader> prevSegmentReaderList, File sourceDir, File targetDir) throws IOException {
		for (SegmentReader prevSegmentReader : prevSegmentReaderList) {
			String segmentId = prevSegmentReader.segmentInfo().getId();
			String filename = IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentId);
			File f1 = new File(sourceDir, filename);
			File f2 = new File(targetDir, filename);
			logger.debug("Copy file {}=>{}", f1.getAbsolutePath(), f2.getAbsolutePath());
			FileUtils.copyFile(f1, f2);
		}

	}

	private void copyPrimaryKeyAndDeleteTemp(File sourceDir, File targetDir) throws IOException {
		logger.debug("COPY primarykey!");
		String indexFilename = IndexFileNames.getIndexFileName(IndexFileNames.primaryKeyMap);
		FileUtils.copyFile(new File(sourceDir, IndexFileNames.primaryKeyMap), new File(targetDir, IndexFileNames.primaryKeyMap));
		FileUtils.copyFile(new File(sourceDir, indexFilename), new File(targetDir, indexFilename));

		String tempPkFilename = IndexFileNames.getTempFileName(IndexFileNames.primaryKeyMap);
		new File(targetDir, tempPkFilename).delete();
		new File(targetDir, IndexFileNames.getIndexFileName(tempPkFilename)).delete();

	}

	/*
	 * 1. 이전 revision을 바탕으로 새 revision의 PK를 비교하여 Merge하고 2. docDeleteSet도 업데이트한다.
	 */
	private void mergePrimaryKeyWithPrevRevision(String segmentId, File prevRevisionDir, File targetRevisionDir) throws IOException {
		logger.debug("MERGE primarykey!");
		//
		// 이전 revision의 pk와 현재 revision의 pk를 머징하고 중복된 문서번호는 deleteSet에 넣는다.
		//
		int indexInterval = collectionContext.indexConfig().getPkTermInterval();
		BitSet revDeleteSet = new BitSet(targetRevisionDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentId));
		File prevRevisionPkFile = new File(prevRevisionDir, IndexFileNames.primaryKeyMap);

		String tempPkFilename = IndexFileNames.getTempFileName(IndexFileNames.primaryKeyMap);
		File tempPkFile = new File(targetRevisionDir, tempPkFilename);
		File newPkFile = new File(targetRevisionDir, IndexFileNames.primaryKeyMap);
		if (tempPkFile.exists()) {
			logger.debug("MERGE PK {} AND {} => {}", new Object[] { prevRevisionPkFile.getPath(), tempPkFile.getPath(), newPkFile.getPath() });
			// 동일세그먼트내에서 이전 rev와 새 rev사이의 중복문서가 발견횟수를 증가시킨다.
			int updateDocumentCount = new PrimaryKeyIndexMerger().merge(prevRevisionPkFile, tempPkFile, newPkFile, indexInterval, revDeleteSet);
			logger.debug("UPDATE SIZE after merge = {}", updateDocumentCount);
			revDeleteSet.save();
			// temp PK .index파일은 pk파일을 만들때 생성된 것으로, temp PK용도는 bulk용도이므로 .index파일이 필요없다.
			// 그러므로 삭제!!

			String tempPkIndexFilename = IndexFileNames.getIndexFileName(tempPkFilename);
			new File(targetRevisionDir, tempPkFilename).delete();
			new File(targetRevisionDir, tempPkIndexFilename).delete();
		}
	}

	

	/*
	 * 색인시 수집된 삭제문서리스트 deleteIdSet를 각 deleteSet에 적용한다. pk파일에 들어있다면 문서가 존재하는 것이므로 deleteSet에 업데이트해준다.
	 */
	private int applyDeleteIdSetToAllSegments(String segmentId, File segmentDir, DeleteIdSet deleteIdSet, PrimaryKeyIndexReader[] prevPkReaderList,
			BitSet[] prevDeleteSetList) throws IOException {

		int deleteDocumentSize = 0;

		if (deleteIdSet == null) {
			return deleteDocumentSize;
		}
		PrimaryKeysToBytesRef primaryKeysToBytesRef = new PrimaryKeysToBytesRef(schema);
		/*
		 * apply delete set. 이번 색인작업을 통해 삭제가 요청된 문서들을 삭제처리한다.
		 */
		PrimaryKeyIndexReader currentPkReader = new PrimaryKeyIndexReader(segmentDir, IndexFileNames.primaryKeyMap);
		BitSet currentDeleteSet = new BitSet(segmentDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentId));
		Iterator<PrimaryKeys> iterator = deleteIdSet.iterator();
		while (iterator.hasNext()) {

			PrimaryKeys ids = iterator.next();
			logger.debug("--- delete id = {}", ids);

			BytesRef bytesRef = primaryKeysToBytesRef.getBytesRef(ids);

			int localDocNo = currentPkReader.get(bytesRef);
			if (localDocNo != -1) {
				// add delete list
				logger.debug("--- delete localDocNo = {} at current segment", localDocNo);
				currentDeleteSet.set(localDocNo);
				deleteDocumentSize++;// deleteSize 증가.
				// 현재 세그먼트에서 삭제된 pk는, 이전 세그먼트에서 찾아볼필요가 없다.
				// 왜냐하면 현재세그먼트와 동일한 pk가 이전 세그먼트에 존재한다면 해당 pk는 이미 삭제리스트에 존재할것이기 때문이다.
				continue;
			}
			// backward matching
			for (int i = prevPkReaderList.length - 1; i >= 0; i--) {
				localDocNo = prevPkReaderList[i].get(bytesRef);
				if (localDocNo != -1) {
					if (!prevDeleteSetList[i].isSet(localDocNo)) {
						// add delete list
						logger.debug("--- delete doc[{}] at segment[{}]", localDocNo, i);
						prevDeleteSetList[i].set(localDocNo);
						deleteDocumentSize++;// deleteSize 증가.
					}
					break;
				}
			}

		}

		currentDeleteSet.save();

		currentPkReader.close();

		return deleteDocumentSize;
	}

	public int segmentSize() {
		if (segmentReaderList == null) {
			return 0;
		}

		return segmentReaderList.size();
	}

	public AnalyzerPool getAnalyzerPool(String analyzerId) {
		return analyzerPoolManager.getPool(analyzerId);
	}

	public void setQueryCounter(Counter queryCounter) {
		if (queryCounter != null) {
			this.queryCounter = queryCounter;
			logger.debug("[{}] Collection set Query counter {}", collectionId, queryCounter);
		} else {
			logger.debug("[{}] Collection Query counter Not Found!", collectionId);
		}
	}

	public Counter queryCounter() {
		return queryCounter;
	}

}
