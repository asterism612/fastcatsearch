package org.fastcatsearch.transport.vo;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.env.Environment;
import org.fastcatsearch.ir.config.CollectionConfig;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus;
import org.fastcatsearch.ir.config.DataInfo;
import org.fastcatsearch.ir.config.DataSourceConfig;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.config.IndexingScheduleConfig;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.fastcatsearch.util.FilePaths;
import org.fastcatsearch.util.JAXBConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamableCollectionContext implements Streamable {
	private static Logger logger = LoggerFactory.getLogger(StreamableCollectionContext.class);
	private Environment environment;
	private CollectionContext collectionContext;
	
	public StreamableCollectionContext(){
	}
	
	public StreamableCollectionContext(CollectionContext collectionContext) {
		this.collectionContext = collectionContext;
	}

	public StreamableCollectionContext(Environment environment) {
		this.environment = environment;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		try {
			String collectionId = input.readString();
			SchemaSetting schemaSetting = JAXBConfigs.readFrom(input, SchemaSetting.class);
			Schema schema = new Schema(schemaSetting);
			CollectionConfig collectionConfig = JAXBConfigs.readFrom(input, CollectionConfig.class);
			IndexConfig indexConfig = JAXBConfigs.readFrom(input, IndexConfig.class);
			DataSourceConfig dataSourceConfig = JAXBConfigs.readFrom(input, DataSourceConfig.class);
			CollectionIndexStatus collectionStatus = JAXBConfigs.readFrom(input, CollectionIndexStatus.class);
			DataInfo dataInfo = JAXBConfigs.readFrom(input, DataInfo.class);
			IndexingScheduleConfig indexingScheduleConfig = JAXBConfigs.readFrom(input, IndexingScheduleConfig.class);
			int lastSegmentId = input.readInt();
			//collectionFilePaths는 현 node에 적합하도록 새로 생성한다. 
			FilePaths collectionFilePaths = environment.filePaths().collectionFilePaths(collectionId);
			this.collectionContext = new CollectionContext(collectionId, collectionFilePaths);
			collectionContext.init(schema, null, collectionConfig, indexConfig, dataSourceConfig, collectionStatus, dataInfo, indexingScheduleConfig, lastSegmentId);
		} catch (JAXBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		//collectionFilePaths 과 workSchema는 전송하지 않는다.
		output.writeString(collectionContext.collectionId());
		try{
			JAXBConfigs.writeTo(output, collectionContext.schema().schemaSetting(), SchemaSetting.class);
			JAXBConfigs.writeTo(output, collectionContext.collectionConfig(), CollectionConfig.class);
			JAXBConfigs.writeTo(output, collectionContext.indexConfig(), IndexConfig.class);
			JAXBConfigs.writeTo(output, collectionContext.dataSourceConfig(), DataSourceConfig.class);
			JAXBConfigs.writeTo(output, collectionContext.indexStatus(), CollectionIndexStatus.class);
			JAXBConfigs.writeTo(output, collectionContext.dataInfo(), DataInfo.class);
			JAXBConfigs.writeTo(output, collectionContext.indexingScheduleConfig(), IndexingScheduleConfig.class);
			output.writeInt(collectionContext.getLastSegmentId());
			
		} catch (JAXBException e) {
			throw new IOException(e);
		}
	}

	public CollectionContext collectionContext(){
		return collectionContext;
	}
}
