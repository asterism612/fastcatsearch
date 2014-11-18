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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.cluster.Node;
import org.fastcatsearch.cluster.NodeService;
import org.fastcatsearch.common.io.Streamable;
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
import org.fastcatsearch.job.result.IndexingJobResult;
import org.fastcatsearch.job.state.IndexingTaskState;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.transport.vo.StreamableCollectionContext;
import org.fastcatsearch.transport.vo.StreamableThrowable;
import org.fastcatsearch.util.CollectionContextUtil;

/**
 * 특정 collection의 index node에서 수행되는 job.
 * 오직 색인작업만 수행하면 전파 및 적용은 하지 않는다. 
 * index node가 아닌 노드에 전달되면 색인을 수행하지 않는다.
 *  
 * */
public class CollectionFullIndexingStepBuildJob extends IndexingJob {

	private static final long serialVersionUID = 7898036370433248984L;

	private CollectionContext collectionContext; 
	
	public CollectionFullIndexingStepBuildJob(){
	}
	
	public CollectionFullIndexingStepBuildJob(CollectionContext collectionContext){
		this.collectionContext = collectionContext;
	}
	
	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		prepare(IndexingType.FULL, "BUILD-INDEX");
		
		
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
			
			SegmentInfo segmentInfo = new SegmentInfo();
			int segmentSize = collectionContext.collectionConfig().getFullIndexingSegmentSize();
			CollectionIndexerable collectionFullIndexer = null;
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
//			MultiThreadCollectionFullIndexer collectionFullIndexer = new MultiThreadCollectionFullIndexer(collectionContext, analyzerPoolManager);
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
			 * 데이터노드가 리로드 완료되었으면 인덱스노드도 리로드 시작.
			 * */
			indexingTaskState.setStep(IndexingTaskState.STEP_FINALIZE);
			
			CollectionContextUtil.saveCollectionAfterIndexing(collectionContext);
			CollectionHandler collectionHandler = irService.loadCollectionHandler(collectionContext);
			Counter queryCounter = irService.queryCountModule().getQueryCounter(collectionId);
			collectionHandler.setQueryCounter(queryCounter);
			CollectionHandler oldCollectionHandler = irService.putCollectionHandler(collectionId, collectionHandler);
			if (oldCollectionHandler != null) {
				logger.info("## [{}] Close Previous Collection Handler", collectionContext.collectionId());
				oldCollectionHandler.close();
			}
			
			int duration = (int) (System.currentTimeMillis() - startTime);
			
			/*
			 * 캐시 클리어.
			 */
			getJobExecutor().offer(new CacheServiceRestartJob());

			IndexStatus indexStatus = collectionContext.indexStatus().getFullIndexStatus();
			indexingLogger.info("[{}] Collection Full Indexing Finished! {} time = {}", collectionId, indexStatus, duration);
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
