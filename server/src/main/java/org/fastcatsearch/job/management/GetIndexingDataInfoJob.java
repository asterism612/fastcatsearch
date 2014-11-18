package org.fastcatsearch.job.management;

import java.io.File;
import java.io.IOException;

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

public class GetIndexingDataInfoJob extends Job implements Streamable {

	private static final long serialVersionUID = -9023882122708815679L;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		String collectionId = getStringArgs();
		
		IndexingDataInfo result = new IndexingDataInfo();
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionContext collectionContext = irService.collectionContext(collectionId);
		if(collectionContext == null){
			//컬렉션이 올바로 로딩되지 않았다면 빈 객체리턴.
			return new JobResult(result);
		}
		
		DataInfo dataInfo = collectionContext.dataInfo();
		
		result.segmentSize = dataInfo.getSegmentSize();
		String revisionUUID = null;
		SegmentInfo lastSegmentInfo = dataInfo.getLastSegmentInfo();
		if(lastSegmentInfo != null){
			revisionUUID = lastSegmentInfo.getUuid();
		}else{
			revisionUUID = "";
		}
		result.revisionUUID = revisionUUID;
		
		int sequence = collectionContext.indexStatus().getSequence();
		result.sequence = sequence;
		File indexFileDir = collectionContext.dataFilePaths().indexDirFile(sequence);
		result.dataPath = new Path(collectionContext.collectionFilePaths().file()).relativise(indexFileDir).getPath();
		
		String diskSize = "";
		
		if(indexFileDir.exists()){
			long byteCount = FileUtils.sizeOfDirectory(indexFileDir);
			diskSize = FileUtils.byteCountToDisplaySize(byteCount);
		}
		result.diskSize = diskSize;
		
		result.documentSize = collectionContext.dataInfo().getDocuments();
		
		String createTime = "";
		SegmentInfo segmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
		if(segmentInfo != null){
			result.createTime = createTime;
		}
		result.createTime = segmentInfo.getCreateTime();
		
		
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

	
	
	
	public static class IndexingDataInfo implements Streamable {
		public int segmentSize;
		public String revisionUUID;
		public int sequence;
		public String dataPath;
		public String diskSize;
		public int documentSize;
		public String createTime;
		
		@Override
		public void readFrom(DataInput input) throws IOException {
			segmentSize = input.readInt();
			revisionUUID = input.readString();
			sequence = input.readInt();
			dataPath = input.readString();
			diskSize = input.readString();
			documentSize = input.readInt();
			createTime = input.readString();
		}

		@Override
		public void writeTo(DataOutput output) throws IOException {
			output.writeInt(segmentSize);
			output.writeString(revisionUUID);
			output.writeInt(sequence);
			output.writeString(dataPath);
			output.writeString(diskSize);
			output.writeInt(documentSize);
			output.writeString(createTime);
		}
		
		
		
		
	}

}
