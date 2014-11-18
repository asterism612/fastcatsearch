package org.fastcatsearch.job.management;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.env.Path;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.DataInfo;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.job.Job;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.settings.SettingFileNames;
import org.fastcatsearch.util.JAXBConfigs;

/**
 * 현재 loading중인 seq의 색인정보가 아닌 restore시 사용될 색인정보. index#/info.xml의 정보를 이용한다.
 * */
public class GetRestorableIndexingDataInfoJob extends Job implements Streamable {

	private static final long serialVersionUID = -3683180705857933635L;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		String collectionId = getStringArgs();
		
		IndexingDataShortInfo result = new IndexingDataShortInfo();
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionContext collectionContext = irService.collectionContext(collectionId);
		if(collectionContext == null){
			//컬렉션이 올바로 로딩되지 않았다면 빈 객체리턴.
			return new JobResult(result);
		}
		
		//이전 시퀀스를 가져온다.
		int prevSequence = collectionContext.getPreviousDataSequence();
		result.sequence = prevSequence;
		File prevIndexFileDir = collectionContext.dataFilePaths().indexDirFile(prevSequence);
		result.dataPath = new Path(collectionContext.collectionFilePaths().file()).relativise(prevIndexFileDir).getPath();
		
		if(prevIndexFileDir.exists()){
			long byteCount = FileUtils.sizeOfDirectory(prevIndexFileDir);
			result.diskSize = FileUtils.byteCountToDisplaySize(byteCount);
		}
		
		File dataInfoFile = new File(prevIndexFileDir, SettingFileNames.dataInfo);
		logger.debug("load dataInfoFile > {}", dataInfoFile.getAbsolutePath());
		DataInfo dataInfo = null;
		try{
			if(dataInfoFile.exists()){
				dataInfo = JAXBConfigs.readConfig(dataInfoFile, DataInfo.class);
				String revisionUUID = null;
				SegmentInfo lastSegmentInfo = dataInfo.getLastSegmentInfo();
				if(lastSegmentInfo != null){
					revisionUUID = lastSegmentInfo.getUuid();
				}else{
					revisionUUID = "";
				}
				result.revisionUUID = revisionUUID;
				
				result.documentSize = dataInfo.getDocuments();
			}
			
		}catch(JAXBException e){
			logger.error("", e);
		}
		
		return new JobResult(result);
	}
	

	@Override
	public void readFrom(DataInput input) throws IOException {
		args = input.readString();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeString(getStringArgs());
	}

	
	
	
	public static class IndexingDataShortInfo implements Streamable {
		public String revisionUUID;
		public int sequence;
		public String dataPath;
		public String diskSize;
		public int documentSize;
		
		@Override
		public void readFrom(DataInput input) throws IOException {
			revisionUUID = input.readString();
			sequence = input.readInt();
			dataPath = input.readString();
			diskSize = input.readString();
			documentSize = input.readInt();
		}

		@Override
		public void writeTo(DataOutput output) throws IOException {
			output.writeString(revisionUUID);
			output.writeInt(sequence);
			output.writeString(dataPath);
			output.writeString(diskSize);
			output.writeInt(documentSize);
		}
		
		
		
		
	}

}
