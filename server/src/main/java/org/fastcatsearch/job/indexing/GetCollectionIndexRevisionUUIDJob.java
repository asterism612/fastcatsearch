package org.fastcatsearch.job.indexing;

import java.io.IOException;

import org.fastcatsearch.common.io.Streamable;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.job.Job;
import org.fastcatsearch.service.ServiceManager;

/**
 * 증분색인시 이전 revisionUUID가 일치하는지 여부확인.
 * 일치하는 노드만 증분색인 데이터 전송 대상이 된다.
 * */
public class GetCollectionIndexRevisionUUIDJob extends Job implements Streamable {

	private static final long serialVersionUID = -9020411832250747477L;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		String collectionId = getStringArgs();
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		CollectionContext collectionContext = irService.collectionContext(collectionId);
		SegmentInfo lastSegmentInfo = collectionContext.dataInfo().getLastSegmentInfo();
		String lastSegmentUUID = lastSegmentInfo.getUuid();
		
		return new JobResult(lastSegmentUUID);
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		args = input.readString();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeString(getStringArgs());
	}

}
