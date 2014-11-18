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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.fastcatsearch.cluster.ClusterUtils;
import org.fastcatsearch.cluster.Node;
import org.fastcatsearch.cluster.NodeJobResult;
import org.fastcatsearch.cluster.NodeService;
import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.control.JobService;
import org.fastcatsearch.control.ResultFuture;
import org.fastcatsearch.db.mapper.IndexingResultMapper.ResultStatus;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.CollectionFullIndexer;
import org.fastcatsearch.ir.CollectionIndexerable;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.MultiThreadCollectionFullIndexer;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IndexingType;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.ir.util.Counter;
import org.fastcatsearch.job.CacheServiceRestartJob;
import org.fastcatsearch.job.cluster.NodeCollectionReloadJob;
import org.fastcatsearch.job.cluster.NodeDirectoryCleanJob;
import org.fastcatsearch.job.result.IndexingJobResult;
import org.fastcatsearch.job.state.IndexingTaskState;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.transport.vo.StreamableCollectionContext;
import org.fastcatsearch.transport.vo.StreamableThrowable;
import org.fastcatsearch.util.CollectionContextUtil;
import org.fastcatsearch.util.FilePaths;

/**
 * 특정 collection의 index node에서 수행되는 job.
 * index node가 아닌 노드에 전달되면 색인을 수행하지 않는다.
 *  
 * */
public class CollectionFullIndexingJob extends IndexingJob {

	private static final long serialVersionUID = 7898036370433248984L;

	private CollectionContext collectionContext; 
	
	public CollectionFullIndexingJob(){
	}
	
	public CollectionFullIndexingJob(CollectionContext collectionContext){
		this.collectionContext = collectionContext;
	}
	
	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		prepare(IndexingType.FULL, "ALL");
		
		
		Throwable throwable = null;
		ResultStatus resultStatus = ResultStatus.RUNNING;
		Object result = null;
		long startTime = System.currentTimeMillis();
		try {
			IRService irService = ServiceManager.getInstance().getService(IRService.class);
			AnalyzerPoolManager analyzerPoolManager = irService.createAnalyzerPoolManager(collectionContext.schema().schemaSetting().getAnalyzerSettingList());			
			//find index node
//			CollectionContext collectionContext = irService.collectionContext(collectionId);
			String indexNodeId = collectionContext.collectionConfig().getIndexNode();
			NodeService nodeService = ServiceManager.getInstance().getService(NodeService.class);
			Node indexNode = nodeService.getNodeById(indexNodeId);
			
			if(!nodeService.isMyNode(indexNode)){
				//Pass job to index node
				//작업수행하지 않음.
				throw new RuntimeException("Invalid index node collection[" + collectionId + "] node[" + indexNodeId + "]");
			}

			if(!updateIndexingStatusStart()) {
				logger.error("Cannot start indexing job. {} : {}", collectionId, indexNodeId);
				resultStatus = ResultStatus.CANCEL;
				return new JobResult();
			}

			/*
			 * Do indexing!!
			 */
			//////////////////////////////////////////////////////////////////////////////////////////
			
			boolean isIndexed = false; 
			int segmentSize = collectionContext.collectionConfig().getFullIndexingSegmentSize();
			CollectionIndexerable collectionFullIndexer = null;
			SegmentInfo segmentInfo = new SegmentInfo();
			if(segmentSize <= 1){
				collectionFullIndexer = new CollectionFullIndexer(segmentInfo, collectionContext, analyzerPoolManager);
			}else{
				List<SegmentInfo> workingSegmentInfoList = new ArrayList<SegmentInfo>(segmentSize);
				SegmentInfo workingSegmentInfo = new SegmentInfo();
				workingSegmentInfoList.add(workingSegmentInfo);
				//순차적 세그먼트 info 를 할당한다.
				for (int inx = 1; inx < segmentSize; inx++) {
					workingSegmentInfo = workingSegmentInfo.getNextSegmentInfo();
					workingSegmentInfoList.add(workingSegmentInfo);
				}
				collectionFullIndexer = new MultiThreadCollectionFullIndexer(segmentInfo, workingSegmentInfoList, collectionContext, analyzerPoolManager);	
			}
			indexer = collectionFullIndexer;
			collectionFullIndexer.setTaskState(indexingTaskState);
			Throwable indexingThrowable = null;
			try {
				collectionFullIndexer.doIndexing();
			}catch(Throwable e){
				indexingThrowable = e;
			} finally {
				if (collectionFullIndexer != null) {
					try{
						isIndexed = collectionFullIndexer.close();
					}catch(Throwable closeThrowable){
						//이전에 이미 발생한 에러가 있다면 close 중에 발생한 에러보다 이전 에러를 throw한다.
						if(indexingThrowable == null){
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
			
			/*
			 * 색인파일 원격복사.
			 */
			indexingTaskState.setStep(IndexingTaskState.STEP_FILECOPY);
			
//			String segmentId = segmentInfo.getId();
			logger.debug("Transfer index data collection[{}] >> {}", collectionId);

			FilePaths indexFilePaths = collectionContext.indexFilePaths();
			File indexDir = indexFilePaths.file();
//				File segmentDir = indexFilePaths.file(segmentId);

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
			
			Set<Node> reloadNodeSet = new HashSet<Node>();
			//데이터노드
			reloadNodeSet.addAll(nodeList);
			//인덱스노드
			reloadNodeSet.add(indexNode);
			//마스터노드 (관리도구에 보여지기 위함) 추가
			reloadNodeSet.add(nodeService.getMasterNode());
			// 
			for(String nodeId : collectionContext.collectionConfig().getSearchNodeList()){
				reloadNodeSet.add(nodeService.getNodeById(nodeId));
			}
			
			/*
			 * 데이터노드에 컬렉션 리로드 요청.
			 */
			NodeCollectionReloadJob reloadJob = new NodeCollectionReloadJob(collectionContext);
			nodeResultList = ClusterUtils.sendJobToNodeSet(reloadJob, nodeService, reloadNodeSet, true);
			for (int i = 0; i < nodeResultList.length; i++) {
				NodeJobResult r = nodeResultList[i];
				logger.debug("node#{} >> {}", i, r);
				if (r.isSuccess()) {
					logger.info("{} Collection reload OK.", r.node());
				}else{
					logger.warn("{} Collection reload Fail.", r.node());
				}
			}
			
//				if (!nodeResult) {
//					throw new FastcatSearchException("Node Collection Reload Failed!");
//				}
			
			indexingTaskState.setStep(IndexingTaskState.STEP_FINALIZE);
			int duration = (int) (System.currentTimeMillis() - startTime);
			
			IndexStatus indexStatus = collectionContext.indexStatus().getFullIndexStatus();
			indexingLogger.info("[{}] Collection Full Indexing Finished! time = {}", collectionId, duration);
			
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
			throw new FastcatSearchException("ERR-00500", throwable, collectionId); // 전체색인실패.
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
	
	@Override
	public void readFrom(DataInput input) throws IOException {
		super.readFrom(input);
		StreamableCollectionContext streamableCollectionContext = new StreamableCollectionContext(environment);
		streamableCollectionContext.readFrom(input);
		this.collectionContext = streamableCollectionContext.collectionContext();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		super.writeTo(output);
		StreamableCollectionContext streamableCollectionContext = new StreamableCollectionContext(collectionContext);
		streamableCollectionContext.writeTo(output);
	}

}
