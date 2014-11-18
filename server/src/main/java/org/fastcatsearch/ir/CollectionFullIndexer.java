package org.fastcatsearch.ir;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.datasource.reader.DataSourceReader;
import org.fastcatsearch.datasource.reader.DefaultDataSourceReaderFactory;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.DataSourceConfig;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.job.indexing.IndexingStopException;

/**
 * 컬렉션의 전체색인을 수행하는 indexer.
 * */
public class CollectionFullIndexer extends AbstractCollectionIndexer {
	
	public CollectionFullIndexer(SegmentInfo segmentInfo, CollectionContext collectionContext, AnalyzerPoolManager analyzerPoolManager) throws IRException {
		super(segmentInfo, collectionContext, analyzerPoolManager);

		init(collectionContext.schema());
	}

	@Override
	protected DataSourceReader createDataSourceReader(File filePath, SchemaSetting schemaSetting) throws IRException{
		DataSourceConfig dataSourceConfig = collectionContext.dataSourceConfig();
		return DefaultDataSourceReaderFactory.createFullIndexingSourceReader(filePath, schemaSetting, dataSourceConfig);
	}

	@Override
	protected void prepare() throws IRException {
		
		// data 디렉토리를 변경한다.
		int newDataSequence = collectionContext.nextDataSequence();

		// 디렉토리 초기화.
		File indexDataDir = collectionContext.collectionFilePaths().dataPaths().indexDirFile(newDataSequence);
		try {
			//FileUtils.deleteDirectory(indexDataDir);
			if(indexDataDir.exists()) {
				FileUtils.forceDelete(indexDataDir);
			}
		} catch (IOException e) {
			throw new IRException(e);
		}

		collectionContext.clearDataInfoAndStatus();
		indexDataDir.mkdirs();
	}

	@Override
	protected boolean done(IndexStatus indexStatus) throws IRException, IndexingStopException {
		int insertCount = workingSegmentInfo.getInsertCount();

		if (insertCount > 0 && !stopRequested) {
//			//이미 동일한 revinfo이므로 재셋팅필요없다.
//			//workingSegmentInfo.updateRevision(revisionInfo);
//			
//			//update index#/info.xml file
//			//addindexing의 updateCollection대신 호출.
			collectionContext.addSegmentInfo(workingSegmentInfo);  
//			//update status.xml file
			collectionContext.updateCollectionStatus(IndexingType.FULL, workingSegmentInfo, startTime, System.currentTimeMillis());
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
}

