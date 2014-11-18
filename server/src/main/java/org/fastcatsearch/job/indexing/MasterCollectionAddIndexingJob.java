package org.fastcatsearch.job.indexing;

import org.fastcatsearch.cluster.Node;
import org.fastcatsearch.cluster.NodeService;
import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.control.ResultFuture;
import org.fastcatsearch.db.mapper.IndexingResultMapper.ResultStatus;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.job.MasterNodeJob;
import org.fastcatsearch.notification.NotificationService;
import org.fastcatsearch.notification.message.IndexingFailNotification;
import org.fastcatsearch.processlogger.IndexingProcessLogger;
import org.fastcatsearch.processlogger.ProcessLoggerService;
import org.fastcatsearch.processlogger.log.IndexingFinishProcessLog;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.transport.vo.StreamableThrowable;

public class MasterCollectionAddIndexingJob extends MasterNodeJob {

	private static final long serialVersionUID = -9030366773507675894L;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		long indexingStartTime = System.currentTimeMillis();
		String collectionId = getStringArgs();
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionContext collectionContext = irService.collectionContext(collectionId);
		if(collectionContext == null) {
			throw new FastcatSearchException("Collection [" + collectionId + "] is not exist.");
		}
		String indexNodeId = collectionContext.collectionConfig().getIndexNode();

		NodeService nodeService = ServiceManager.getInstance().getService(NodeService.class);
		Node indexNode = nodeService.getNodeById(indexNodeId);

		// 증분색인용 context를 준비한다.
		CollectionContext newCollectionContext = collectionContext.copy();
		
		CollectionAddIndexingJob collectionIndexingJob = new CollectionAddIndexingJob(newCollectionContext);
		collectionIndexingJob.setArgs(collectionId);
		collectionIndexingJob.setScheduled(isScheduled);
		
		logger.info("Request add indexing job to index node[{}] >> {}", indexNodeId, indexNode);
		ResultFuture jobResult = nodeService.sendRequest(indexNode, collectionIndexingJob);
		if (jobResult != null) {
			Object obj = jobResult.take();
		} else {
			long endTime = System.currentTimeMillis();
			Streamable result = null;//new StreamableThrowable(t);
			ProcessLoggerService processLoggerService = ServiceManager.getInstance().getService(ProcessLoggerService.class);
			processLoggerService.log(IndexingProcessLogger.class, new IndexingFinishProcessLog(collectionId, IndexingType.ADD, "ALL", ResultStatus.FAIL, indexingStartTime, endTime,
					isScheduled(), result));

			NotificationService notificationService = ServiceManager.getInstance().getService(NotificationService.class);
			IndexingFailNotification indexingFinishNotification = new IndexingFailNotification(collectionId, IndexingType.ADD, "ALL", ResultStatus.FAIL, indexingStartTime, endTime,
					result);
			notificationService.sendNotification(indexingFinishNotification);
		}
		
		return new JobResult();
	}

}
