package org.fastcatsearch.http.action.management.collections;

import java.io.Writer;
import java.util.Collection;

import org.fastcatsearch.http.ActionMapping;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.AuthAction;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.ShardContext;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.util.ResponseWriter;

@ActionMapping("/management/collections/index-data-status")
public class GetCollectionIndexDataStatusAction extends AuthAction {

	@Override
	public void doAuthAction(ActionRequest request, ActionResponse response) throws Exception {

		String collectionId = request.getParameter("collectionId");

		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
		Collection<ShardContext> shardContextList = collectionHandler.collectionContext().getShardContextList();
		int shardSize = shardContextList.size();

		Writer writer = response.getWriter();
		ResponseWriter resultWriter = getDefaultResponseWriter(writer);

		resultWriter.object().key("indexDataStatus");
		resultWriter.array();
		if (shardSize > 0) {
			for (ShardContext shardContext : shardContextList) {
				int documentSize = shardContext.dataInfo().getDocuments();
				resultWriter.object()
					.key("shardId").value(shardContext.shardId())
					.key("documentSize").value(documentSize)
				.endObject();
			}
		} else {

		}
		resultWriter.endArray().endObject();

		resultWriter.done();
	}

}