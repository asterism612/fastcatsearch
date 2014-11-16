package org.fastcatsearch.ir.config;

import java.util.Date;

import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.ir.util.Formatter;
import org.fastcatsearch.util.FilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionContext {
	protected static Logger logger = LoggerFactory.getLogger(CollectionContext.class);
	
	private String id;
	private FilePaths collectionFilePaths;
	private Schema schema;
	private SchemaSetting workSchemaSetting;
	private CollectionConfig collectionConfig;
	private IndexConfig indexConfig;
	private DataSourceConfig dataSourceConfig;
	private CollectionIndexStatus collectionIndexStatus;
	private DataInfo dataInfo;
	private IndexingScheduleConfig indexingScheduleConfig;
	
	public CollectionContext(String collectionId, FilePaths collectionFilePaths) {
		this.id = collectionId;
		this.collectionFilePaths = collectionFilePaths;
	}

	public void init(Schema schema, SchemaSetting workSchemaSetting, CollectionConfig collectionConfig, IndexConfig indexConfig, DataSourceConfig dataSourceConfig
			, CollectionIndexStatus collectionStatus, DataInfo dataInfo, IndexingScheduleConfig indexingScheduleConfig){
		this.schema = schema;
		this.workSchemaSetting = workSchemaSetting;
		this.collectionConfig = collectionConfig;
		this.indexConfig = indexConfig;
		this.dataSourceConfig = dataSourceConfig;
		this.collectionIndexStatus = collectionStatus;
		this.dataInfo = dataInfo;
		this.indexingScheduleConfig = indexingScheduleConfig;
	}
	
	
	public CollectionContext copy(){
		CollectionContext collectionContext = new CollectionContext(id, collectionFilePaths);
		collectionContext.schema = schema;
		collectionContext.workSchemaSetting = workSchemaSetting;
		collectionContext.collectionConfig = collectionConfig;
		collectionContext.indexConfig = indexConfig;
		collectionContext.dataSourceConfig = dataSourceConfig;
		collectionContext.collectionIndexStatus = collectionIndexStatus.copy();
		collectionContext.dataInfo = dataInfo.copy();
		collectionContext.indexingScheduleConfig = indexingScheduleConfig;
		return collectionContext;
	}
	
	public String collectionId(){
		return id;
	}
	
	//collection home 디렉토리를 리턴한다.
	public FilePaths collectionFilePaths(){
		return collectionFilePaths;
	}
	
	// data/ 를 리턴한다.
	public FilePaths dataFilePaths(){
		return collectionFilePaths.dataPaths();
	}
	
	// data/index# 를 리턴한다.
	public FilePaths indexFilePaths(){
		int currentDataSequence = collectionIndexStatus.getSequence();
		return dataFilePaths().indexFilePaths(currentDataSequence);
	}
	
	public Schema schema(){
		return schema;
	}
	public void setSchema(Schema schema){
		this.schema = schema;
	}
	public SchemaSetting workSchemaSetting(){
		return workSchemaSetting;
	}
	
	public void setWorkSchemaSetting(SchemaSetting schemaSetting){
		workSchemaSetting = schemaSetting;
	}
	
	public CollectionConfig collectionConfig(){
		return collectionConfig;
	}
	
	public IndexConfig indexConfig(){
		return indexConfig;
	}
	
	public DataSourceConfig dataSourceConfig(){
		return dataSourceConfig;
	}
	
	public void setDataSourceConfig(DataSourceConfig config){
		this.dataSourceConfig = config;
	}
	
	public CollectionIndexStatus indexStatus(){
		return collectionIndexStatus;
	}
	
	public DataInfo dataInfo(){
		return dataInfo;
	}

	public String getLastIndexTime() {
		if (collectionIndexStatus.getAddIndexStatus() != null) {
			return collectionIndexStatus.getAddIndexStatus().getStartTime();
		} else {
			if (collectionIndexStatus.getFullIndexStatus() != null) {
				return collectionIndexStatus.getFullIndexStatus().getStartTime();
			}
		}
		return null;
	}
	
	public void updateCollectionStatus(IndexingType indexingType, SegmentInfo segmentInfo, long startTime, long endTime){
		IndexStatus indexStatus = null;
		if(indexingType == IndexingType.FULL){
			indexStatus = collectionIndexStatus.getFullIndexStatus();
			if(indexStatus == null){
				indexStatus = new IndexStatus();
				collectionIndexStatus.setFullIndexStatus(indexStatus);
			}
			//전체색인시 증분색인 status는 지워준다.
			collectionIndexStatus.setAddIndexStatus(null);
		}else{
			indexStatus = collectionIndexStatus.getAddIndexStatus();
			if(indexStatus == null){
				indexStatus = new IndexStatus();
				collectionIndexStatus.setAddIndexStatus(indexStatus);
			}
		}
		indexStatus.setDocumentCount(segmentInfo.getDocumentCount());
		indexStatus.setInsertCount(segmentInfo.getInsertCount());
		indexStatus.setUpdateCount(segmentInfo.getUpdateCount());
		indexStatus.setDeleteCount(segmentInfo.getDeleteCount());
		indexStatus.setStartTime(Formatter.formatDate(new Date(startTime)));
		indexStatus.setEndTime(Formatter.formatDate(new Date(endTime)));
		indexStatus.setDuration(Formatter.getFormatTime(endTime - startTime));
	}

	public IndexingScheduleConfig indexingScheduleConfig(){
		return indexingScheduleConfig;
	}
	
	public int nextDataSequence(){
		int currentDataSequence = collectionIndexStatus.getSequence();
		int dataSequenceCycle = collectionConfig.getDataPlanConfig().getDataSequenceCycle();
		int nextDataSequence = (currentDataSequence + 1) % dataSequenceCycle;
		collectionIndexStatus.setSequence(nextDataSequence);
		return nextDataSequence;
	}
	
	public int getPreviousDataSequence(){
		int currentDataSequence = collectionIndexStatus.getSequence();
		int dataSequenceCycle = collectionConfig.getDataPlanConfig().getDataSequenceCycle();
		if(currentDataSequence == 0){
			return dataSequenceCycle - 1;
		}else{
			return currentDataSequence - 1;
		}
	}
	
	public int getIndexSequence(){
		return collectionIndexStatus.getSequence();
	}
	
	public void updateSegmentInfo(SegmentInfo segmentInfo) {
		dataInfo.updateSegmentInfo(segmentInfo);
	}
	public void addSegmentInfo(SegmentInfo segmentInfo) {
		dataInfo.addSegmentInfo(segmentInfo);
	}
	
	public void clearDataInfoAndStatus() {
		dataInfo = new DataInfo();
		collectionIndexStatus.clear();
	}
}
