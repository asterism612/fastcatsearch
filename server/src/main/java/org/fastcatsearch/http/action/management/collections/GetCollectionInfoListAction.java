package org.fastcatsearch.http.action.management.collections;

import java.io.File;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.env.Path;
import org.fastcatsearch.http.ActionAuthority;
import org.fastcatsearch.http.ActionAuthorityLevel;
import org.fastcatsearch.http.ActionMapping;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.AuthAction;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionConfig;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionsConfig.Collection;
import org.fastcatsearch.ir.config.DataInfo;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.util.ResponseWriter;

@ActionMapping(value = "/management/collections/collection-info-list", authority = ActionAuthority.Collections, authorityLevel = ActionAuthorityLevel.NONE)
public class GetCollectionInfoListAction extends AuthAction {

	@Override
	public void doAuthAction(ActionRequest request, ActionResponse response) throws Exception {
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		
		String collectionListStr = request.getParameter("collectionId", "");
		
		List<String> collections = null;
		
		if(!"".equals(collectionListStr)) {
			collections = Arrays.asList(collectionListStr.replaceAll(" ", "").split(","));
		}
	
		List<Collection> collectionList = irService.getCollectionList();
		
		Writer writer = response.getWriter();
		ResponseWriter responseWriter = getDefaultResponseWriter(writer);
		responseWriter.object().key("collectionInfoList").array("collectionInfo");
		for(Collection collection : collectionList){
			String collectionId = collection.getId();
			
			//원하는 컬렉션만 골라낼 때
			if (collections != null && !collections.contains(collectionId)) {
				continue;
			}
			
			CollectionContext collectionContext = irService.collectionContext(collectionId);
			if(collectionContext == null){
				continue;
			}
			CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
			boolean isActive = collectionHandler != null && collectionHandler.isLoaded();
			CollectionConfig collectionConfig = collectionContext.collectionConfig();
			DataInfo dataInfo = collectionContext.dataInfo();
			String revisionUUID = null;
			SegmentInfo lastSegmentInfo = dataInfo.getLastSegmentInfo();
			if(lastSegmentInfo != null){
				revisionUUID = lastSegmentInfo.getUuid();
			}else{
				revisionUUID = "";
			}
			int sequence = collectionContext.indexStatus().getSequence();
			
			responseWriter.object();
			
			{//simple-info
				responseWriter
				.key("id").value(collectionId);
			}
			
			{//normal-info
				responseWriter
				.key("isActive").value(isActive)
				.key("name").value(collectionConfig.getName())
				.key("sequence").value(sequence)
				.key("revisionUUID").value(revisionUUID)
				.key("indexNode").value(collectionConfig.getIndexNode())
				.key("dataNodeList").value(join(collectionConfig.getDataNodeList()))
				.key("searchNodeList").value(join(collectionConfig.getSearchNodeList()));
			}
			
			{//detail-info
				File indexFileDir = collectionContext.dataFilePaths().indexDirFile(sequence);
				int documentSize = collectionContext.dataInfo().getDocuments();
				int segmentSize = dataInfo.getSegmentSize();
				String diskSize = "";
				if(indexFileDir.exists()){
					long byteCount = FileUtils.sizeOfDirectory(indexFileDir);
					diskSize = FileUtils.byteCountToDisplaySize(byteCount);
				}
				String dataPath = new Path(collectionContext.collectionFilePaths().file()).relativise(indexFileDir).getPath();
				String createTime = "";
				SegmentInfo segmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
				if(segmentInfo != null){
					createTime = segmentInfo.getCreateTime();
				}
				responseWriter
				.key("documentSize").value(documentSize)
				.key("segmentSize").value(segmentSize)
				.key("diskSize").value(diskSize)
				.key("dataPath").value(dataPath)
				.key("createTime").value(createTime);
			}
			responseWriter.endObject();
		}
		responseWriter.endArray().endObject();
		responseWriter.done();
	}

	public String join(List<String> list) {
		String joinString = "";
		if (list != null) {
			for (int i = 0; i < list.size(); i++) {
				if(i > 0){
					joinString += ", ";
				}
				joinString += list.get(i);
			}
		}

		return joinString;
	}
}
