package org.fastcatsearch.ir;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.datasource.reader.DataSourceReader;
import org.fastcatsearch.datasource.reader.DefaultDataSourceReaderFactory;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.DataSourceConfig;
import org.fastcatsearch.ir.index.SelectedIndexList;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.job.indexing.IndexingStopException;

public class CollectionFullDocumentStorer extends AbstractCollectionIndexer {

	public CollectionFullDocumentStorer(CollectionContext collectionContext) throws IRException {
		super(collectionContext, null, new SelectedIndexList());
		init(collectionContext.schema());
	}

	@Override
	protected DataSourceReader createDataSourceReader(File filePath, SchemaSetting schemaSetting) throws IRException{
		DataSourceConfig dataSourceConfig = collectionContext.dataSourceConfig();
		return DefaultDataSourceReaderFactory.createFullIndexingSourceReader(filePath, schemaSetting, dataSourceConfig);
	}

	@Override
	protected void prepare() throws IRException {
		workingSegmentInfo = new SegmentInfo();
		
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
	protected boolean done(IndexStatus indexStatus) throws IRException, IndexingStopException {
		int insertCount = workingSegmentInfo.getInsertCount();

		if (insertCount > 0 && !stopRequested) {
			collectionContext.indexStatus().setFullIndexStatus(indexStatus);
			
			return false;
		}else{
			if(!stopRequested){
				logger.info("[{}] Document Store Canceled due to no documents.", collectionContext.collectionId());
				throw new IndexingStopException(collectionContext.collectionId()+" Indexing Canceled due to no documents.");
			}else{
				logger.info("[{}] Document Store Canceled due to Stop Requested!", collectionContext.collectionId());
				throw new IndexingStopException(collectionContext.collectionId()+" Indexing Canceled due to Stop Requested");
			}
			
		}
	}

}
