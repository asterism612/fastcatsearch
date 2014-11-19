package org.fastcatsearch.ir.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <data-info documents="7500" updates="15" deletes="60"> <segment id="0"
 * base="0" > <revision id="0" documents="1000" inserts="990" updates="5"
 * deletes="0" createTime="2013-06-15 15:20:00"> </segment> <segment id="1"
 * base="1000" > <revision id="2" documents="5000" inserts="990" updates="5"
 * deletes="10" createTime="2013-06-15 16:20:00"> </segment> <segment id="2"
 * base="6000" > <revision id="1" documents="1500" inserts="990" updates="5"
 * deletes="50" createTime="2013-06-15 16:30:00"/> </segment> </data-info>
 * */

@XmlRootElement(name = "data-info")
@XmlType(propOrder = { "segmentInfoList", "deletes", "updates", "documents" })
public class DataInfo {
	private static Logger logger = LoggerFactory.getLogger(DataInfo.class);

	private int documents;
	private int updates;
	private int deletes;

	// TODO id 순서대로 list에 추가되도록 adapter만들어야한다.
	private List<SegmentInfo> segmentInfoList;

	public DataInfo() {
		segmentInfoList = new ArrayList<SegmentInfo>();
	}

	public DataInfo copy() {
		DataInfo dataInfo = new DataInfo();
		dataInfo.documents = this.documents;
		dataInfo.updates = this.updates;
		dataInfo.deletes = this.deletes;
		dataInfo.segmentInfoList = new ArrayList<SegmentInfo>();
		for (SegmentInfo segmentInfo : segmentInfoList) {
			dataInfo.segmentInfoList.add(segmentInfo.copy());
		}
		return dataInfo;
	}

	public void update(int documents, int updates, int deletes) {
		this.documents = documents;
		this.updates = updates;
		this.deletes = deletes;
	}

	public void addUpdate(int documents, int updates, int deletes) {
		this.documents += documents;
		this.updates += updates;
		this.deletes += deletes;
	}

	public void addSegmentInfo(SegmentInfo segmentInfo) {
		logger.debug("#### addSegmentInfo >> {}", segmentInfo);
		segmentInfoList.add(segmentInfo);
		//RevisionInfo revisionInfo = segmentInfo.getRevisionInfo();
		addUpdate(segmentInfo.getInsertCount(), segmentInfo.getUpdateCount(), segmentInfo.getDeleteCount());
	}

	public void updateSegmentInfo(SegmentInfo segmentInfo) {
		if (segmentInfoList.contains(segmentInfo)) {
			int index = segmentInfoList.indexOf(segmentInfo);
			SegmentInfo prevSegmentInfo = segmentInfoList.get(index);
			//마지막 세그먼트를 덮어쓴다.
			prevSegmentInfo.update(segmentInfo);
//			RevisionInfo revisionInfo = segmentInfo.getRevisionInfo();
			addUpdate(segmentInfo.getInsertCount(), segmentInfo.getUpdateCount(), segmentInfo.getDeleteCount());
			//존재할 경우 리비전만 업데이트한다.
		} else {
			addSegmentInfo(segmentInfo);
		}
	}

	@XmlAttribute
	public int getDocuments() {
		return documents;
	}

	public void setDocuments(int documents) {
		this.documents = documents;
	}

	@XmlAttribute
	public int getUpdates() {
		return updates;
	}

	public void setUpdates(int updates) {
		this.updates = updates;
	}

	@XmlAttribute
	public int getDeletes() {
		return deletes;
	}

	public void setDeletes(int deletes) {
		this.deletes = deletes;
	}

	@XmlElement(name = "segment")
	public List<SegmentInfo> getSegmentInfoList() {
		return segmentInfoList;
	}

	public void setSegmentInfoList(List<SegmentInfo> segmentInfoList) {
		//여기서 id가 0,1,2,순으로 정렬이 보장되야한다.
		Collections.sort(segmentInfoList);
		this.segmentInfoList = segmentInfoList;
	}

	public int getSegmentSize() {
		return segmentInfoList.size();
	}

	public int getLastSegmentNumber() {
		return segmentInfoList.size() - 1;
	}

	public SegmentInfo getLastSegmentInfo() {
		if (segmentInfoList.size() == 0) {
			return null;
		}
		return segmentInfoList.get(segmentInfoList.size() - 1);
	}

	public String toString() {
		return ("[DataInfo] documents[" + documents + "] updates[" + updates + "] deletes[" + deletes + "] segments[" + segmentInfoList + "]");
	}

	/**
	 * <segment id="0" base="0" revision="0"> <revision documents="1000"
	 * updates="5" deletes="0" createTime="2013-06-15 15:20:00" /> </segment>
	 * */
	@XmlRootElement(name = "segment")
	@XmlType(propOrder = { "createTime", "deleteCount", "updateCount", "insertCount", "documentCount", "baseNumber", "uuid", "id" })
	// , "revision"
	public static class SegmentInfo implements Comparable<SegmentInfo>{
		private String id;
		private int baseNumber;
		// private int revision;
//		private RevisionInfo revisionInfo;
		private String uuid;
		private int documentCount;
		private int insertCount;
		private int updateCount;
		private int deleteCount;
		private String createTime;
		

		public SegmentInfo() {
			this.id = "0";
//			revisionInfo = new RevisionInfo();
			uuid = generateUUID();
		}
		
		private String generateUUID(){
			return UUID.randomUUID().toString().replaceAll("-", "");
		}

		public SegmentInfo(String id, int baseNumber) {
			this.id = id;
			this.baseNumber = baseNumber;
		}

		@Override
		public boolean equals(Object other) {
			// id가 동일하면 같은 SegmentInfo이다.
			return other != null && this.id.equals(((SegmentInfo) other).id);
		}

		public SegmentInfo copy() {
			SegmentInfo segmentInfo = new SegmentInfo();
			segmentInfo.id = id;
			segmentInfo.baseNumber = baseNumber;
//			segmentInfo.revisionInfo = revisionInfo.copy();
			segmentInfo.uuid = uuid;
			segmentInfo.documentCount = documentCount;
			segmentInfo.insertCount = insertCount;
			segmentInfo.updateCount = updateCount;
			segmentInfo.deleteCount = deleteCount;
			segmentInfo.createTime = createTime;
			return segmentInfo;
		}

		public void resetRevisionInfo() {
			insertCount = 0;
			updateCount = 0;
			deleteCount = 0;
		}

		public String toString() {
			return "[SegmentInfo] id[" + id + "] base[" + baseNumber + "] uuid[" + uuid + "] documents[" + documentCount + "] inserts[" + insertCount + "] updates[" + updateCount
					+ "] deletes[" + deleteCount + "] createTime[" + createTime + "]";
		}

		public void update(SegmentInfo segmentInfo) {
			this.id = segmentInfo.id;
			this.baseNumber = segmentInfo.baseNumber;
//			this.revisionInfo = segmentInfo.revisionInfo;
			this.uuid = segmentInfo.uuid;
			this.documentCount = segmentInfo.documentCount;
			this.insertCount = segmentInfo.insertCount;
			this.updateCount = segmentInfo.updateCount;
			this.deleteCount = segmentInfo.deleteCount;
			this.createTime = segmentInfo.createTime;
		}

		// id와 baseNumber는 변경되지 않는다.
		// TODO 상위 data info 의 문서수도 변경되야 한다.
//		public void updateRevision(RevisionInfo revisionInfo) {
//			logger.debug("updateRevision > {}", revisionInfo);
//			if (revisionInfo == null) {
//				return;
//			}
//
//			// this.revision = revisionInfo.id;
//			if (this.revisionInfo != null) {
//				// 누적숫자로 유지한다.
//				revisionInfo.documentCount += this.revisionInfo.documentCount;
//				
////				revisionInfo.updateCount += this.revisionInfo.updateCount;
////				revisionInfo.deleteCount += this.revisionInfo.deleteCount;
//			}
//			this.revisionInfo = revisionInfo;
//		}

		@XmlAttribute
		public String getId() {
			return id;
		}

		public int getIntId() {
			return Integer.parseInt(id);
		}

		public void setId(String id) {
			this.id = id;
		}

		@XmlAttribute(name = "base")
		public int getBaseNumber() {
			return baseNumber;
		}

		public void setBaseNumber(int baseNumber) {
			this.baseNumber = baseNumber;
		}

//		public int getRevision() {
//			return revisionInfo.getId();
//		}

//		public String getRevisionName() {
//			return Integer.toString(revisionInfo.getId());
//		}

//		@XmlElement(name = "revision")
//		public RevisionInfo getRevisionInfo() {
//			return revisionInfo;
//		}
//
//		public void setRevisionInfo(RevisionInfo revisionInfo) {
//			this.revisionInfo = revisionInfo;
//		}
		
		@XmlAttribute(name = "uuid")
		public String getUuid() {
			return uuid;
		}

		public void setUuid(String uuid) {
			this.uuid = uuid;
		}

		@XmlAttribute(name = "documents")
		public int getDocumentCount() {
			return documentCount;
		}

		public void setDocumentCount(int documentCount) {
			this.documentCount = documentCount;
		}

		@XmlAttribute(name = "inserts")
		public int getInsertCount() {
			return insertCount;
		}

		public void setInsertCount(int insertCount) {
			this.insertCount = insertCount;
		}

		@XmlAttribute(name = "updates")
		public int getUpdateCount() {
			return updateCount;
		}

		public void setUpdateCount(int updateCount) {
			this.updateCount = updateCount;
		}

		@XmlAttribute(name = "deletes")
		public int getDeleteCount() {
			return deleteCount;
		}

		public void setDeleteCount(int deleteCount) {
			this.deleteCount = deleteCount;
		}

		@XmlAttribute
		public String getCreateTime() {
			return createTime;
		}

		public void setCreateTime(String createTime) {
			this.createTime = createTime;
		}

		public String getNextId() {
			return Integer.toString(Integer.parseInt(id) + 1);
		}

//		public int nextRevision() {
//			if (uuid != null) {
//				return getNextId()
//			} else {
//				revisionInfo = new RevisionInfo();
//				return revisionInfo.getId();
//			}
//		}

		public int getNextBaseNumber() {
			if(documentCount > 0) {
				return baseNumber + documentCount;
			} else {
				return -1;
			}
		}

//		public SegmentInfo getNextSegmentInfo() {
//			SegmentInfo nextSegmentInfo = new SegmentInfo();
//			nextSegmentInfo.id = getNextId();
////			nextSegmentInfo.baseNumber = getNextBaseNumber();
//			return nextSegmentInfo;
//		}

		@Override
		public int compareTo(SegmentInfo o) {
			return Integer.parseInt(id) - Integer.parseInt(o.id);
		}
		
	}

	/**
	 * <revision id="1" ref="0" documents="1000" insertCount="990" updates="10"
	 * deletes="0" createTime="2013-06-15 15:20:00">
	 * */
//	@XmlType(propOrder = { "createTime", "deleteCount", "updateCount", "insertCount", "documentCount", "ref", "id" })
//	@XmlRootElement(name = "revision")
//	public static class RevisionInfo {
//
//		private int id;
//		private String uuid;
//		private int ref;
//		private int documentCount;
//		private int insertCount;
//		private int updateCount;
//		private int deleteCount;
//		private String createTime;
//
//		public RevisionInfo() {
//			uuid = generateUUID();
//		}
//
//		private String generateUUID(){
//			return UUID.randomUUID().toString().replaceAll("-", "");
//		}
//		
//		public RevisionInfo(int id, String uuid, int documentCount, int insertCount, int updateCount, int deleteCount, String createTime) {
//			this.id = id;
//			this.uuid = uuid;
//			this.ref = id;
//			this.documentCount = documentCount;
//			this.insertCount = insertCount;
//			this.updateCount = updateCount;
//			this.deleteCount = deleteCount;
//			this.createTime = createTime;
//		}
//
//		public boolean isAppend() {
//			return id > 0;
//		}
//
//		public RevisionInfo copy() {
//			RevisionInfo revisionInfo = new RevisionInfo();
//			revisionInfo.id = id;
//			revisionInfo.uuid = uuid;
//			revisionInfo.ref = ref;
//			revisionInfo.documentCount = documentCount;
//			revisionInfo.insertCount = insertCount;
//			revisionInfo.updateCount = updateCount;
//			revisionInfo.deleteCount = deleteCount;
//			revisionInfo.createTime = createTime;
//			return revisionInfo;
//		}
//
//		public int nextRevision() {
//			id++;
//			uuid = generateUUID();
//			return id;
//		}
//
//		// ref와 revision을 동일하게 맞춘다.
//		public void setRefWithRevision() {
//			ref = id;
//		}
//
//		public String toString() {
//			return "[RevisionInfo] id[" + id + "] uuid[" + uuid + "] ref[" + ref + "] documents[" + documentCount + "] inserts[" + insertCount + "] updates[" + updateCount
//					+ "] deletes[" + deleteCount + "] createTime[" + createTime + "]";
//		}
//
//		@XmlAttribute(name = "id")
//		public int getId() {
//			return id;
//		}
//
//		public void setId(int id) {
//			this.id = id;
//		}
//
//		@XmlAttribute(name = "uuid")
//		public String getUuid() {
//			return uuid;
//		}
//
//		public void setUuid(String uuid) {
//			this.uuid = uuid;
//		}
//
//		@XmlAttribute(name = "ref")
//		public int getRef() {
//			return ref;
//		}
//
//		public void setRef(int ref) {
//			this.ref = ref;
//		}
//
//		@XmlAttribute(name = "documents")
//		public int getDocumentCount() {
//			return documentCount;
//		}
//
//		public void setDocumentCount(int documentCount) {
//			this.documentCount = documentCount;
//		}
//
//		@XmlAttribute(name = "inserts")
//		public int getInsertCount() {
//			return insertCount;
//		}
//
//		public void setInsertCount(int insertCount) {
//			this.insertCount = insertCount;
//		}
//
//		@XmlAttribute(name = "updates")
//		public int getUpdateCount() {
//			return updateCount;
//		}
//
//		public void setUpdateCount(int updateCount) {
//			this.updateCount = updateCount;
//		}
//
//		@XmlAttribute(name = "deletes")
//		public int getDeleteCount() {
//			return deleteCount;
//		}
//
//		public void setDeleteCount(int deleteCount) {
//			this.deleteCount = deleteCount;
//		}
//
//		@XmlAttribute
//		public String getCreateTime() {
//			return createTime;
//		}
//
//		public void setCreateTime(String createTime) {
//			this.createTime = createTime;
//		}
//
//		public void add(RevisionInfo revisionInfo) {
//			this.documentCount += revisionInfo.documentCount;
//			this.insertCount += revisionInfo.insertCount;
//			this.updateCount += revisionInfo.updateCount;
//			this.deleteCount += revisionInfo.deleteCount;
//		}
//	}

}
