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

package org.fastcatsearch.ir.document;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.zip.Deflater;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.field.Field;
import org.fastcatsearch.ir.index.IndexWriteInfoList;
import org.fastcatsearch.ir.index.WriteInfoLoggable;
import org.fastcatsearch.ir.io.BufferedFileOutput;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 문서를 한개씩 압축하여 기록한다.
 * 입력되는 Document들을 파일로 기록하는 클래스.
 * 
 * 생성되는파일은 2개인데 문서데이터파일, 데이터파일내 문서위치를 기록한 문서위치파일이다.
 * 
 * 문서번호는 append일때에는 이전 리비전의 다음부터 생성한다.아니면 0.
 * 차후에 DocumentReader에서 읽을때는 읽으려는 문서번호에서 docbaseNo을 빼서 내부문서번호로
 * 변경한뒤 읽는다.
 * 
 * @author sangwook.song
 * 
 */
public class DocumentWriter implements WriteInfoLoggable {
	private static Logger logger = LoggerFactory.getLogger(DocumentWriter.class);

	private List<FieldSetting> fields;
	private BufferedFileOutput docOutput;
	private BufferedFileOutput positionOutput;
	private byte[] workingBuffer;
	private BytesDataOutput fbaos;
	private int totalCount;  //누적문서갯수.
	private Deflater compressor;
	private int count; //현 색인시 추가문서갯수.
	
	public DocumentWriter(SchemaSetting schemaSetting, File dir) throws IOException, IRException {
		
//		boolean isAppend = revisionInfo.isAppend();
		
		compressor = new Deflater(Deflater.BEST_SPEED);
		fields = schemaSetting.getFieldSettingList();
		
		docOutput = new BufferedFileOutput(dir, IndexFileNames.docStored);
		positionOutput = new BufferedFileOutput(dir, IndexFileNames.docPosition);

		fbaos = new BytesDataOutput(3 * 1024 * 1024); //초기 3Mb로 시작.
		workingBuffer = new byte[1024];

//		if (isAppend) {
//			IndexInput docInput = new BufferedFileInput(dir, IndexFileNames.docStored);
//			totalCount = docInput.readInt();
//			docInput.close();
//		} else {
			docOutput.writeInt(0); // document count
//		}

	}


	public int write(Document document) throws IOException, IRException {
		fbaos.reset();
		long docStartPosition = docOutput.position();
		positionOutput.writeLong(docStartPosition);
		
		for (int i = 0; i < document.size(); i++) {
			Field f = document.get(i);
			//필드가 null이면 데이터없는 것으로 처리
			FieldSetting fieldSetting = fields.get(i);
			if(f == null || f.isNull() || !fieldSetting.isStore()){
				fbaos.writeBoolean(false);
			} else {
				fbaos.writeBoolean(true);
				//TODO 차후 필드별 압축으로 변경한다. 필드 데이터를 업데이트 하려면 필드별로 관리해야함.
//				fbaos.writeBoolean(fieldSetting.isCompress());
				f.writeRawTo(fbaos);
			}
		}
		
		compressor.reset();
		compressor.setInput(fbaos.array(), 0, (int) fbaos.position());
		compressor.finish();

		long pos = docOutput.position();
		docOutput.writeInt(0); // 압축데이터길이 임시기록.

		int compressedDataLength = 0;
		while (!compressor.finished()) {
			int count = compressor.deflate(workingBuffer);
			docOutput.writeBytes(workingBuffer, 0, count);
			compressedDataLength += count;
		}
		
		long lastPos = docOutput.position();
		// 길이헤더를 정확한 데이터로 기록한다.
		docOutput.seek(pos);
		docOutput.writeInt(compressedDataLength);
		docOutput.seek(lastPos);
		
		count++;
		return totalCount++;
	}

	public int totalCount(){
		return totalCount;
	}
	
	public void close() throws IOException {
		logger.debug("DocumentWriter close() count={}", count);

		// write header
		if(count > 0){
			docOutput.seek(0);
			docOutput.writeInt(totalCount);
		}
		docOutput.close();
		
		positionOutput.close();
	}

	@Override
	public void getIndexWriteInfo(IndexWriteInfoList writeInfoList) {
		writeInfoList.setDocumentSize(totalCount);
		writeInfoList.add(docOutput.getWriteInfo());
		writeInfoList.add(positionOutput.getWriteInfo());
		
	}
}
