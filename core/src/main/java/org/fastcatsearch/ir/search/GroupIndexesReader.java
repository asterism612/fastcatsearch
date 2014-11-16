package org.fastcatsearch.ir.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.settings.GroupIndexSetting;
import org.fastcatsearch.ir.settings.Schema;

public class GroupIndexesReader extends SelectableIndexesReader<GroupIndexReader, GroupIndexSetting> {
	
	public GroupIndexesReader(){ }
	
	public GroupIndexesReader(Schema schema, File dir) throws IOException, IRException{
		indexSettingList = schema.schemaSetting().getGroupIndexSettingList();
		int indexCount = indexSettingList == null ? 0 : indexSettingList.size();
		
		//색인파일열기.
		readerList = new ArrayList<GroupIndexReader>(indexCount);
		for (int i = 0; i < indexCount; i++) {
			GroupIndexSetting setting = indexSettingList.get(i);
			GroupIndexReader reader = null;
			try{
				reader = new GroupIndexReader(setting, schema.fieldSettingMap(), dir);
			}catch(Exception e){
				logger.error("그룹색인 {}로딩중 에러 >> {}", setting.getId(), e);
			}
			readerList.add(reader);
		}
		
	}
	
	@Override
	public GroupIndexesReader clone(){
		GroupIndexesReader reader = new GroupIndexesReader();
		reader.indexSettingList = indexSettingList;
    	reader.readerList = new ArrayList<GroupIndexReader>(readerList.size());
    	for (GroupIndexReader r : readerList) {
    		GroupIndexReader newReader = null;
    		if(r != null){
    			newReader = r.clone();
    		}
    		reader.readerList.add(newReader);
		}
    	
    	return reader;
    }
	
	@Override
	protected GroupIndexReader cloneReader(int sequence) {
		return readerList.get(sequence).clone();
	}

}
