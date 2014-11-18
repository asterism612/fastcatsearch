/*
 * Copyright (c) 2013 Websquared, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *     swsong - initial API and implementation
 */

package org.fastcatsearch.job.indexing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.cluster.ClusterUtils;
import org.fastcatsearch.cluster.Node;
import org.fastcatsearch.cluster.NodeJobResult;
import org.fastcatsearch.cluster.NodeService;
import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.control.JobService;
import org.fastcatsearch.control.ResultFuture;
import org.fastcatsearch.db.mapper.IndexingResultMapper.ResultStatus;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.CollectionAddIndexer;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.index.IndexWriteInfoList;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.ir.search.SegmentReader;
import org.fastcatsearch.job.CacheServiceRestartJob;
import org.fastcatsearch.job.cluster.NodeCollectionUpdateJob;
import org.fastcatsearch.job.cluster.NodeDirectoryCleanJob;
import org.fastcatsearch.job.result.IndexingJobResult;
import org.fastcatsearch.job.state.IndexingTaskState;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.transport.vo.StreamableThrowable;
import org.fastcatsearch.util.FilePaths;

/**
 * 특정 collection의 index node에서 수행되는 job.
 * index node가 아닌 노드에 전달되면 색인을 수행하지 않는다.
 *  
 * */
public class CollectionAddIndexingJob extends IndexingJob {

	private static final long serialVersionUID = 7898036370433248984L;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		prepare(IndexingType.ADD, "ALL");
		
		Throwable throwable = null;
		ResultStatus resultStatus = ResultStatus.RUNNING;
		Object result = null;
		long startTime = System.currentTimeMillis();
		try {
			IRService irService = ServiceManager.getInstance().getService(IRService.class);
			
			//find index node
			CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
			CollectionContext collectionContext = irService.collectionContext(collectionId);
			if(collectionContext == null) {
				throw new FastcatSearchException("Collection [" + collectionId + "] is not exist.");
			}
			
			AnalyzerPoolManager analyzerPoolManager = collectionHandler.analyzerPoolManager();			
			
			String indexNodeId = collectionContext.collectionConfig().getIndexNode();
			NodeService nodeService = ServiceManager.getInstance().getService(NodeService.class);
			Node indexNode = nodeService.getNodeById(indexNodeId);
			
			if(!nodeService.isMyNode(indexNode)){
				//Pass job to index node
//				nodeService.sendRequest(indexNode, this);
				//작업수행하지 않음.
				throw new RuntimeException("Invalid index node collection[" + collectionId + "] node[" + indexNodeId + "]");
			}
			
			if(!updateIndexingStatusStart()) {
				logger.error("Cannot start indexing job. {} : {}", collectionId, indexNodeId);
				resultStatus = ResultStatus.CANCEL;
				return new JobResult();
			}

			//증분색인은 collection handler자체를 수정하므로 copy하지 않는다.
//			collectionContext = collectionContext.copy();
			/*
			 * Do indexing!!
			 */
			//////////////////////////////////////////////////////////////////////////////////////////
			SegmentInfo lastSegmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
			if(lastSegmentInfo == null){
				//색인이 안된상태이다.
				throw new FastcatSearchException("Cannot index collection. It has no full indexing information. collectionId = "+collectionContext.collectionId());
			}
			String lastRevisionUUID = lastSegmentInfo.getUuid();
			
			boolean isIndexed = false;
			SegmentInfo workingSegmentInfo = lastSegmentInfo.getNextSegmentInfo();
			CollectionAddIndexer collectionIndexer = new CollectionAddIndexer(workingSegmentInfo, collectionContext, analyzerPoolManager);
			indexer = collectionIndexer;
			collectionIndexer.setTaskState(indexingTaskState);
			Throwable indexingThrowable = null;
			try {
				indexer.doIndexing();
			}catch(Throwable e){
				indexingThrowable = e;
			} finally {
				if (collectionIndexer != null) {
					try {
						isIndexed = collectionIndexer.close();
					} catch (Throwable closeThrowable) {
						// 이전에 이미 발생한 에러가 있다면 close 중에 발생한 에러보다 이전 에러를 throw한다.
						if (indexingThrowable == null) {
							indexingThrowable = closeThrowable;
						}
					}
				}
				if(indexingThrowable != null){
					throw indexingThrowable;
				}
			}
			if(!isIndexed && stopRequested){
				//여기서 끝낸다.
				throw new IndexingStopException();
			}
			
			
			
			
			
			
			
			
			//TODO 삭제요청 아이디를 이미 적용해버렸으므로, 다른 세그먼트에 전달해줄 것이 없다.
			// 현 세그먼트는 적용되었다고 해도, 타 노드의 이전 세그먼트는 어떻게 삭제문서를 적용할것인가.  
			
			//방안: 세그먼트를 전달시 삭제문서도 같이 전달... 그렇다면 updateCollection을 수행하기 전에 전달해야한다.
			
			//고려할점 : 문서가 0인지 삭제문서가 있는지 확인후 보낸다. 
			// 문서가 0이고 삭제문서가 있다면, segment삭제후, 삭제문서만 보내고, 
			// 문서도 0, 삭제도 0 이라면 아무것도 보내지않고 여기서 모두 삭제하고 마무리 짓는다.
			
			
			
			//결국 세그먼트가 모두 전파된후, 인덱스 업데이트는 동시에 수행된다.
			
			

			/* 
			 * 2014-11-17 swsong 
			 * 색인시는 세그먼트 파일을 만들어만 주고 외부에서 세그먼트를 기존 인덱스에 붙여준다. 
			 */
			collectionHandler.updateCollection(collectionContext, workingSegmentInfo, collectionIndexer.segmentDir());
			
			
			
			
			
			
			
			/*
			 * 색인파일 원격복사.
			 */
			indexingTaskState.setStep(IndexingTaskState.STEP_FILECOPY);
			
//			IndexWriteInfoList indexWriteInfoList = collectionIndexer.indexWriteInfoList();
			SegmentInfo segmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
			if(segmentInfo != null) {
//				String segmentId = segmentInfo.getId();
				logger.debug("Transfer index data collection[{}] >> {}", collectionId, segmentInfo);
			
				FilePaths indexFilePaths = collectionContext.indexFilePaths();
				File indexDir = indexFilePaths.file();
				
				List<Node> nodeList = new ArrayList<Node>(nodeService.getNodeById(collectionContext.collectionConfig().getDataNodeList()));
				//색인노드가 data node에 추가되어있다면 제거한다.
				nodeList.remove(nodeService.getMyNode());
				
				// 색인전송할디렉토리를 먼저 비우도록 요청.segmentDir
				File relativeDataDir = environment.filePaths().relativise(indexDir);
				NodeDirectoryCleanJob cleanJob = new NodeDirectoryCleanJob(relativeDataDir);

				NodeJobResult[] nodeResultList = null;
				nodeResultList = ClusterUtils.sendJobToNodeList(cleanJob, nodeService, nodeList, false);
				
				//성공한 node만 전송.
				nodeList = new ArrayList<Node>();
				for (int i = 0; i < nodeResultList.length; i++) {
					NodeJobResult r = nodeResultList[i];
					logger.debug("node#{} >> {}", i, r);
					if (r.isSuccess()) {
						nodeList.add(r.node());
					}else{
						logger.warn("Do not send index file to {}", r.node());
					}
				}
				
				if(nodeList.size() > 0) {
					//색인노드만 있으면 이부분을 건너뛴다.
					
					/*
					 * lastRevisionUUID가 일치하는 보낼노드가 존재하는지 확인한다.
					 * 존재한다면 mirror sync file을 만든다.
					 * 
					 * */
					GetCollectionIndexRevisionUUIDJob getRevisionUUIDJob = new GetCollectionIndexRevisionUUIDJob();
					getRevisionUUIDJob.setArgs(collectionId);
					nodeResultList = ClusterUtils.sendJobToNodeList(getRevisionUUIDJob, nodeService, nodeList, false);
					
					//성공한 node만 전송.
					nodeList = new ArrayList<Node>();
					for (int i = 0; i < nodeResultList.length; i++) {
						NodeJobResult r = nodeResultList[i];
						logger.debug("node#{} >> {}", i, r);
						if (r.isSuccess()) {
							String uuid = (String) r.result();
							if(lastRevisionUUID.equals(uuid)){
								nodeList.add(r.node());
							}else{
								logger.error("{} has different uuid > {}", r.node(), uuid);
							}
						}else{
							logger.warn("Cannot get revision information > {}", r.node());
						}
					}
					
					// 색인된 Segment 파일전송.
					TransferIndexFileMultiNodeJob transferJob = new TransferIndexFileMultiNodeJob(indexDir, nodeList);
					ResultFuture resultFuture = JobService.getInstance().offer(transferJob);
					Object obj = resultFuture.take();
					if(resultFuture.isSuccess() && obj != null){
						nodeResultList = (NodeJobResult[]) obj;
					}else{
						
					}
					
					//성공한 node만 전송.
					nodeList = new ArrayList<Node>();
					for (int i = 0; i < nodeResultList.length; i++) {
						NodeJobResult r = nodeResultList[i];
						logger.debug("node#{} >> {}", i, r);
						if (r.isSuccess()) {
							nodeList.add(r.node());
						}else{
							logger.warn("Do not send index file to {}", r.node());
						}
					}
					
					
					if(stopRequested){
						throw new IndexingStopException();
					}
					
					/*
					 * 데이터노드에 컬렉션 리로드 요청.
					 */
					NodeCollectionUpdateJob reloadJob = new NodeCollectionUpdateJob(collectionContext);
					nodeResultList = ClusterUtils.sendJobToNodeList(reloadJob, nodeService, nodeList, false);
					for (int i = 0; i < nodeResultList.length; i++) {
						NodeJobResult r = nodeResultList[i];
						logger.debug("node#{} >> {}", i, r);
						if (r.isSuccess()) {
							logger.info("{} Collection reload OK.", r.node());
						}else{
							logger.warn("{} Collection reload Fail.", r.node());
						}
					}
				}
			}
			
			indexingTaskState.setStep(IndexingTaskState.STEP_FINALIZE);
			int duration = (int) (System.currentTimeMillis() - startTime);
			
			/*
			 * 캐시 클리어.
			 */
			getJobExecutor().offer(new CacheServiceRestartJob());

			IndexStatus indexStatus = collectionContext.indexStatus().getAddIndexStatus();
			indexingLogger.info("[{}] Collection Add Indexing Finished! {} time = {}", collectionId, indexStatus, duration);
			logger.info("== SegmentStatus ==");
			collectionHandler.printSegmentStatus();
			logger.info("===================");
			
			result = new IndexingJobResult(collectionId, indexStatus, duration);
			resultStatus = ResultStatus.SUCCESS;
			indexingTaskState.setStep(IndexingTaskState.STEP_END);
			return new JobResult(result);
			
		} catch (IndexingStopException e){
			if(stopRequested){
				resultStatus = ResultStatus.STOP;
			}else{
				resultStatus = ResultStatus.CANCEL;
			}
			result = new IndexingJobResult(collectionId, null, (int) (System.currentTimeMillis() - startTime), false);
			return new JobResult(result);
		} catch (Throwable e) {
			indexingLogger.error("[" + collectionId + "] Indexing", e);
			throwable = e;
			resultStatus = ResultStatus.FAIL;
			throw new FastcatSearchException("ERR-00501", throwable, collectionId); // 색인실패.
		} finally {
			Streamable streamableResult = null;
			if (throwable != null) {
				streamableResult = new StreamableThrowable(throwable);
			} else if (result instanceof Streamable) {
				streamableResult = (Streamable) result;
			}

			updateIndexingStatusFinish(resultStatus, streamableResult);
		}

	}

}
