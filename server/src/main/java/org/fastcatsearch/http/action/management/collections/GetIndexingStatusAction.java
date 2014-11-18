package org.fastcatsearch.http.action.management.collections;

import java.io.File;
import java.io.Writer;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.env.Path;
import org.fastcatsearch.http.ActionAuthority;
import org.fastcatsearch.http.ActionAuthorityLevel;
import org.fastcatsearch.http.ActionMapping;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.AuthAction;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.DataInfo;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.util.ResponseWriter;

@ActionMapping(value = "/management/collections/indexing-status", authority = ActionAuthority.Collections, authorityLevel = ActionAuthorityLevel.READABLE)
public class GetIndexingStatusAction extends AuthAction {

	@Override
	public void doAuthAction(ActionRequest request, ActionResponse response) throws Exception {
		
		String collectionId = request.getParameter("collectionId");
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionContext collectionContext = irService.collectionContext(collectionId);
		
		
		Writer writer = response.getWriter();
		ResponseWriter responseWriter = getDefaultResponseWriter(writer);
		responseWriter.object()
		.key("collectionId").value(collectionId);
		
		DataInfo dataInfo = collectionContext.dataInfo();
		
		int segmentSize = dataInfo.getSegmentSize();
		String revisionUUID = null;
		SegmentInfo lastSegmentInfo = dataInfo.getLastSegmentInfo();
		if(lastSegmentInfo != null){
			revisionUUID = lastSegmentInfo.getUuid();
		}else{
			revisionUUID = "";
		}
		
		responseWriter.key("segmentSize").value(segmentSize)
		.key("revisionUUID").value(revisionUUID);
		
		int sequence = collectionContext.indexStatus().getSequence();
		File indexFileDir = collectionContext.dataFilePaths().indexDirFile(sequence);
		String dataPath = new Path(collectionContext.collectionFilePaths().file()).relativise(indexFileDir).getPath();
		responseWriter.key("sequence").value(sequence)
			.key("dataPath").value(dataPath);
		String diskSize = "";
		
		if(indexFileDir.exists()){
			long byteCount = FileUtils.sizeOfDirectory(indexFileDir);
			diskSize = FileUtils.byteCountToDisplaySize(byteCount);
		}
		responseWriter.key("diskSize").value(diskSize);
		
		int documentSize = collectionContext.dataInfo().getDocuments();
		responseWriter.key("documentSize").value(documentSize);
		
		String createTime = "";
		SegmentInfo segmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
		if(segmentInfo != null){
			createTime = segmentInfo.getCreateTime();
		}
		responseWriter.key("createTime").value(createTime);
		
		responseWriter.endObject();
		
		responseWriter.done();
	}

}
