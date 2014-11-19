package org.fastcatsearch.ir.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.io.BufferedFileInput;
import org.fastcatsearch.ir.io.DataRef;
import org.fastcatsearch.ir.io.IndexInput;
import org.fastcatsearch.ir.io.StreamInputRef;
import org.fastcatsearch.ir.search.SegmentReader;
import org.fastcatsearch.ir.settings.FieldIndexSetting;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.fastcatsearch.ir.settings.Schema;

/**
 * 멀티밸류이면 indexOutput에 long 값으로 데이터의 ptr이 적혀있다. -1이면 값이없는경우이다. 머징시 file ptr가
 * 이동되므로, base file ptr을 더해서 전체적으로 shift해주어야 한다. 싱글밸류이면, indexOutput에 고정길이 데이터가
 * 기록되어있다.
 * */
public class FieldIndexesMerger {

	private Schema schema;
	private List<FieldIndexSetting> fieldIndexSettingList;
	private List<File> segmentDirList;

	public FieldIndexesMerger(Schema schema, File indexDir,
			List<SegmentReader> segmentReaderList) {
		
		this.schema = schema;
		fieldIndexSettingList = schema.schemaSetting()
				.getFieldIndexSettingList();
		this.segmentDirList = new ArrayList<File>(segmentReaderList.size());
		for (SegmentReader r : segmentReaderList) {
			segmentDirList.add(r.segmentDir());
		}
		
		
		
	}

	public void merge() {
		
		Map<String, FieldSetting> fieldSettingMap = schema.fieldSettingMap();
		
		//필드별로 하나씩.
		for (FieldIndexSetting fieldIndexSetting : fieldIndexSettingList) {
			String id = fieldIndexSetting.getId();
			String refId = fieldIndexSetting.getRef();
			FieldSetting refFieldSetting = fieldSettingMap.get(refId);
			int size = fieldIndexSetting.getSize();
			int dataSize = refFieldSetting.getByteSize(size);
			boolean isMultiValue = refFieldSetting.isMultiValue();
			
			//세그먼트 별로 하나씩.
			for (File segmentDir : segmentDirList) {
				File dataFile = new File(segmentDir, IndexFileNames.getFieldIndexFileName(id));
				File multiValueFile = new File(segmentDir, IndexFileNames.getMultiValueFileName(IndexFileNames.getFieldIndexFileName(id)));
				IndexInput dataInput = null;
				IndexInput multiValueInput = null;
				if(isMultiValue){
		    		try {
						multiValueInput = new BufferedFileInput(multiValueFile);
					} catch (IOException e) {
						e.printStackTrace();
					}
		    	}else{
		    	}
			}
		}
	}

	

}
