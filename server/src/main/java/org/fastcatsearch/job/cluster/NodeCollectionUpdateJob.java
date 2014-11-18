package org.fastcatsearch.job.cluster;

import java.io.File;
import java.io.IOException;

import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionIndexStatus.IndexStatus;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.BitSet;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.job.CacheServiceRestartJob;
import org.fastcatsearch.job.Job;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.transport.vo.StreamableCollectionContext;
import org.fastcatsearch.util.CollectionContextUtil;

/**
 * 증분색인으로 생성된 세그먼트를 컬렉션에 적용한다.
 * 세그먼트 로드 및 삭제문서를 이전 세그먼트에 적용.  
 * */
public class NodeCollectionUpdateJob extends Job implements Streamable {
	private static final long serialVersionUID = 7222232821891387399L;

	private CollectionContext collectionContext;

	public NodeCollectionUpdateJob() {
	}

	public NodeCollectionUpdateJob(CollectionContext collectionContext) {
		this.collectionContext = collectionContext;
	}

	@Override
	public JobResult doRun() throws FastcatSearchException {

		try {
			
			String collectionId = collectionContext.collectionId();
			SegmentInfo segmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
			
			File segmentDir = collectionContext.dataFilePaths().segmentFile(collectionContext.getIndexSequence(), segmentInfo.getId());
//			int revision = segmentInfo.getRevision();
//			File revisionDir = new File(segmentDir, Integer.toString(revision));
			
//			RevisionInfo revisionInfo = segmentInfo.getRevisionInfo();
//			boolean revisionAppended = revisionInfo.getId() > 0;
			boolean revisionHasInserts = segmentInfo.getInsertCount() > 0;
			logger.debug("컬렉션 증분업데이트 실행! segment={}, hasInserts={}", segmentInfo.getId(), revisionHasInserts);
			
			// sync파일을 append해준다.
//			if(revisionAppended){
//				if(revisionHasInserts){
//					logger.debug("revision이 추가되어, mirror file 적용!");
//					File mirrorSyncFile = new File(revisionDir, IndexFileNames.mirrorSync);
//					new MirrorSynchronizer().applyMirrorSyncFile(mirrorSyncFile, revisionDir);
//				}
//			}
			
			CollectionContextUtil.saveCollectionAfterIndexing(collectionContext);
			
			IRService irService = ServiceManager.getInstance().getService(IRService.class);
			CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
			
			logger.debug("segment가 추가되어, 추가 및 적용합니다.{}", segmentInfo);
//			collectionHandler.addSegmentApplyCollection(segmentInfo, segmentDir);
			collectionHandler.updateCollection(collectionContext, segmentInfo, segmentDir);
			
//			if(revisionAppended){
//				logger.debug("revision이 추가되어, 세그먼트를 업데이트합니다.{}", segmentInfo);
//				collectionHandler.updateSegmentApplyCollection(segmentInfo, segmentDir);
//			}else{
//				logger.debug("segment가 추가되어, 추가 및 적용합니다.{}", segmentInfo);
//				collectionHandler.addSegmentApplyCollection(segmentInfo, segmentDir);
//			}
			
			logger.info("== [{}] SegmentStatus ==", collectionId);
			collectionHandler.printSegmentStatus();
			logger.info("===================");
			IndexStatus indexStatus = collectionContext.indexStatus().getAddIndexStatus();
			logger.info("[{}] Collection Index Status > {}", collectionId, indexStatus);

			
			/*
			 * 캐시 클리어.
			 */
			getJobExecutor().offer(new CacheServiceRestartJob());
			return new JobResult(true);

		} catch (Exception e) {
			logger.error("", e);
			throw new FastcatSearchException("ERR-00525", e);
		}

	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		StreamableCollectionContext streamableCollectionContext = new StreamableCollectionContext(environment);
		streamableCollectionContext.readFrom(input);
		this.collectionContext = streamableCollectionContext.collectionContext();
		
		//TODO 
		
		
		//삭제리스트 객체도 받는다. 
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		StreamableCollectionContext streamableCollectionContext = new StreamableCollectionContext(collectionContext);
		streamableCollectionContext.writeTo(output);
	}

}
