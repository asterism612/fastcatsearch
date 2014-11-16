package org.fastcatsearch.ir.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.settings.GroupIndexSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 그룹인덱스에 대한 색인클래스.
 * */
public class GroupIndexesWriter implements WriteInfoLoggable {
	private static Logger logger = LoggerFactory.getLogger(GroupIndexesWriter.class);
	private GroupIndexWriter[] groupIndexWriterList;
	private int indexSize;

	public GroupIndexesWriter(Schema schema, File dir, SegmentInfo segmentInfo, IndexConfig indexConfig) throws IOException, IRException {
		this(schema, dir, segmentInfo, indexConfig, null);
	}

	public GroupIndexesWriter(Schema schema, File dir, SegmentInfo segmentInfo, IndexConfig indexConfig, List<String> indexIdList) throws IOException, IRException {
		List<GroupIndexSetting> groupIndexSettingList = schema.schemaSetting().getGroupIndexSettingList();
		int totalSize = groupIndexSettingList == null ? 0 : groupIndexSettingList.size();

		List<GroupIndexWriter> list = new ArrayList<GroupIndexWriter>();
		for (int i = 0; i < totalSize; i++) {
			GroupIndexSetting indexSetting = groupIndexSettingList.get(i);
			if (indexIdList == null || indexIdList.contains(indexSetting.getId())) {
				GroupIndexWriter groupIndexWriter = new GroupIndexWriter(indexSetting, schema.fieldSettingMap(), schema.fieldSequenceMap(), dir, segmentInfo, indexConfig);
				list.add(groupIndexWriter);
			}
		}
		groupIndexWriterList = list.toArray(new GroupIndexWriter[0]);
		indexSize = groupIndexWriterList.length;
	}

	public void write(Document document) throws IOException {
		for (int i = 0; i < indexSize; i++) {
			groupIndexWriterList[i].write(document);
		}
	}

	public void flush() throws IOException {
		for (int i = 0; i < indexSize; i++) {
			groupIndexWriterList[i].flush();
		}
	}

	public void close() throws IOException {
		for (int i = 0; i < indexSize; i++) {
			groupIndexWriterList[i].close();
		}
	}

	@Override
	public void getIndexWriteInfo(IndexWriteInfoList writeInfoList) {
		for (int i = 0; i < indexSize; i++) {
			groupIndexWriterList[i].getIndexWriteInfo(writeInfoList);
		}
	}
}
