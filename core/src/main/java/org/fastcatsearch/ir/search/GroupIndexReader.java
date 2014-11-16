/*
 * Copyright 2013 Websquared, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fastcatsearch.ir.search;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.document.PrimaryKeyIndexReader;
import org.fastcatsearch.ir.io.DataRef;
import org.fastcatsearch.ir.io.FixedDataInput;
import org.fastcatsearch.ir.io.IOUtil;
import org.fastcatsearch.ir.io.SequencialDataInput;
import org.fastcatsearch.ir.io.StreamInputRef;
import org.fastcatsearch.ir.io.VariableDataInput;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.fastcatsearch.ir.settings.GroupIndexSetting;



/**
 * 
 * 
 * @author sangwook.song
 *
 */
public class GroupIndexReader extends ReferencableIndexReader {
	
	private SequencialDataInput groupKeyInput;
	protected int groupKeySize;
	
	public GroupIndexReader() {}
	
	public GroupIndexReader(GroupIndexSetting groupIndexSetting, Map<String, FieldSetting> fieldSettingMap, File segmentDir) throws IOException, IRException{
		String id = groupIndexSetting.getId();
		String refId = groupIndexSetting.getRef();
		FieldSetting refFieldSetting = fieldSettingMap.get(refId);
		
		File dataFile = new File(segmentDir, IndexFileNames.getGroupIndexFileName(id));
		File multiValueFile = new File(segmentDir, IndexFileNames.getMultiValueFileName(IndexFileNames.getGroupIndexFileName(id)));
    	
		init(id, refFieldSetting, dataFile, multiValueFile, IOUtil.SIZE_OF_INT);
		
		if(refFieldSetting.isVariableField()){
			groupKeyInput = new VariableDataInput(segmentDir, IndexFileNames.getGroupKeyFileName(id));
		}else{
			int dataSize = refFieldSetting.getByteSize();
			groupKeyInput = new FixedDataInput(segmentDir, IndexFileNames.getGroupKeyFileName(id), dataSize);
		}
		
//		File revisionDir = new File(dir, Integer.toString(revision));
		PrimaryKeyIndexReader pkReader = new PrimaryKeyIndexReader(segmentDir, IndexFileNames.getGroupKeyMapFileName(id));
		groupKeySize = pkReader.count();
		pkReader.close();
//		logger.debug("Group {} >> keysize:{}", id, groupKeySize);

	}
	
	//특정 그룹필드의 키값을 읽어온다.
	public boolean readKey(int groupNo, BytesRef bytesRef) throws IOException{
		return groupKeyInput.read(bytesRef, groupNo);
	}
	
	//각 그룹의 key 갯수
	public int getGroupKeySize() {
		return groupKeySize;
	}
	
	@Override
	public GroupIndexReader clone(){
		GroupIndexReader reader = new GroupIndexReader();
		reader.indexId = indexId;
		reader.dataInput = dataInput.clone();
		reader.groupKeyInput = groupKeyInput.clone();
		if(isMultiValue){
			reader.multiValueInput = multiValueInput.clone();
			reader.dataRef = new StreamInputRef(reader.multiValueInput, dataSize);
		}else{
			reader.dataRef = new DataRef(dataSize);
		}
		reader.dataSize = dataSize;
		reader.groupKeySize = groupKeySize;
		reader.isMultiValue = isMultiValue;
		return reader;
	}
	
	@Override
	public void close() throws IOException{
		super.close();
		groupKeyInput.close();
	}

	
	
}





