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

package org.fastcatsearch.ir.index;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.field.Field;
import org.fastcatsearch.ir.field.FieldDataWriter;
import org.fastcatsearch.ir.io.BufferedFileOutput;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.ir.io.IndexOutput;
import org.fastcatsearch.ir.settings.FieldIndexSetting;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 단일 필드인덱스에 대한 색인클래스. 하위에 여러필드를 가질수 있다.
 * 
 * @author sangwook.song
 * 
 */
public class FieldIndexWriter implements WriteInfoLoggable {
	private static Logger logger = LoggerFactory.getLogger(FieldIndexWriter.class);
	private String indexId;
	private IndexOutput fieldIndexOutput;
	private IndexOutput multiValueOutput;
	private boolean isMultiValue;
	private int limitSize;
	private int fieldSequence;
	private boolean isIgnoreCase;

	public FieldIndexWriter(FieldIndexSetting fieldIndexSetting, Map<String, FieldSetting> fieldSettingMap, Map<String, Integer> fieldSequenceMap,
			File dir) throws IOException, IRException {
		String id = fieldIndexSetting.getId();
		this.indexId = id;
		fieldIndexOutput = new BufferedFileOutput(dir, IndexFileNames.getFieldIndexFileName(id));

		String refFieldId = fieldIndexSetting.getRef();
		fieldSequence = fieldSequenceMap.get(refFieldId);
		FieldSetting refFieldSetting = fieldSettingMap.get(refFieldId);
		limitSize = fieldIndexSetting.getSize(); // 색인시 제한 길이. bytesize가 아님.

		isIgnoreCase = fieldIndexSetting.isIgnoreCase();

		isMultiValue = refFieldSetting.isMultiValue();
		if (isMultiValue) {
			multiValueOutput = new BufferedFileOutput(dir, IndexFileNames.getMultiValueFileName(IndexFileNames.getFieldIndexFileName(id)));
		}

	}

	public void write(Document document) throws IOException, IRException {

		Field field = document.get(fieldSequence);

		if (!field.isFixedSize() && limitSize <= 0) {
			throw new IRException("가변길이필드는 필드색인이 불가능합니다. 필드색인SIZE 필요. 필드 = " + indexId + ". 현재 field index size = " + limitSize);
		}

		if (isIgnoreCase) {

			// TODO
			// 필드 객체자체를 바꾸면 다음 index writer에서 혼동되므로 clone한 객체를 바꿔야한다.

		}
//		if (isIgnoreCase) {
//			logger.debug("field index write IGNORECASE1 {}", field.getDataString());
////			field.toUpperCase();
//		}
//		logger.debug("field index write IGNORECASE2 {} >> {}", indexId, field.getDataString());

		if (isMultiValue) {
			long ptr = multiValueOutput.position();
			int multiValueCount = field.getMultiValueCount();

			if (multiValueCount > 0) {
				fieldIndexOutput.writeLong(ptr);
				multiValueOutput.writeVInt(multiValueCount);
				// 정해진 길이가 있다면 해당 길이로 자른다.
				field.writeFixedDataTo(multiValueOutput, limitSize, isIgnoreCase);
			} else {
				fieldIndexOutput.writeLong(-1);
			}

		} else {
			field.writeFixedDataTo(fieldIndexOutput, limitSize, isIgnoreCase);
			
		}
	}

	public void flush() throws IOException {
		fieldIndexOutput.flush();

		if (isMultiValue) {
			multiValueOutput.flush();
		}
	}

	public void close() throws IOException {
		fieldIndexOutput.close();

		if (isMultiValue) {
			multiValueOutput.close();
		}
	}

	@Override
	public void getIndexWriteInfo(IndexWriteInfoList writeInfoList) {
		writeInfoList.add(fieldIndexOutput.getWriteInfo());
		if (isMultiValue) {
			writeInfoList.add(multiValueOutput.getWriteInfo());
		}
	}
}
