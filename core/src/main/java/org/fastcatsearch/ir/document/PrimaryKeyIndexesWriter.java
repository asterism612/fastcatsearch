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

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.field.Field;
import org.fastcatsearch.ir.io.BitSet;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.ir.settings.PrimaryKeySetting;
import org.fastcatsearch.ir.settings.RefSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.util.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * pk맵파일은 pk와 내부문서번호를 맵핑시킨파일인데, 다음번 색인시 추가되는 pk를 이 파일에서 찾아, 삭제문서리스트에 추가해야한다.
 * @author sangwook.song
 * 
 */
public class PrimaryKeyIndexesWriter {
	private static Logger logger = LoggerFactory.getLogger(PrimaryKeyIndexesWriter.class);

	private boolean hasPrimaryKey;
	private BytesDataOutput pkbaos;
	private BitSet deleteSet;
	private LargePrimaryKeyIndexWriter indexWriter;
	private int updateDocCount;
	private PrimaryKeySetting primaryKeySetting;
	private int[] primaryKeyFieldIdList;

	
	int MEMORY_LIMIT = 64 * 1024 * 1024; //적절은 64M
	int CHECK_COUNT = 100000;
	int count;
	public PrimaryKeyIndexesWriter(Schema schema, File segmentDir, SegmentInfo segmentInfo, IndexConfig indexConfig) throws IOException, IRException {
		String segmentId = segmentDir.getName();
//		boolean isAppend = revisionInfo.isAppend();
//		File segmentDir = IndexFileNames.getRevisionDir(dir, segmentInfo.getId());
		
		primaryKeySetting = schema.schemaSetting().getPrimaryKeySetting();
		
		if (primaryKeySetting != null &&  primaryKeySetting.getFieldList() != null) {
			List<RefSetting> refList = primaryKeySetting.getFieldList();
			
			if(refList != null && refList.size() > 0){
				primaryKeyFieldIdList = new int[refList.size()];
				
				int sequence = 0;
				for (RefSetting refSetting : refList) {
					String fieldId = refSetting.getRef();
					primaryKeyFieldIdList[sequence] = schema.getFieldSequence(fieldId);
					logger.debug("pk field [{}]{} >> {}", sequence, fieldId, primaryKeyFieldIdList[sequence]);
					sequence++;
				}
				
				hasPrimaryKey = true;
			}else{
				//pk 없음.
				return;
			}
		}
		//
		// 증분수집일 경우, pk를 나중에 쉽게 읽을 수 있도록 임시벌크파일 형태로 기록한다.
		//
//		if (isAppend) {
//			indexWriter = new LargePrimaryKeyIndexWriter(segmentDir, IndexFileNames.getTempFileName(IndexFileNames.primaryKeyMap), indexConfig.getPkTermInterval(), indexConfig.getPkBucketSize());
//		} else {
			// 전체색인의 경우는 이후에 다시 작업할 일이 없으므로, 완전한 pk map파일로 기록한다.
			indexWriter = new LargePrimaryKeyIndexWriter(segmentDir, IndexFileNames.primaryKeyMap, indexConfig.getPkTermInterval(), indexConfig.getPkBucketSize());
//		}

		pkbaos = new BytesDataOutput(1024); //초기 1kb로 시작.
		deleteSet = new BitSet(segmentDir, IndexFileNames.docDeleteSet, true);
		
//		String docDeleteSetName = IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentId);
//		if (isAppend) {
//			File prevRevisionDir = IndexFileNames.getRevisionDir(dir, revisionInfo.getRef());
//			// copy prev revision's delete.set
//			// 증분색인의 append일 경우에는 이전 revision의 deleteSet을 가져와서 사용한다.
//			// DocumentWriter.close()시 이전 rev와 새 rev의 중복되는 문서를 delete처리해준다.
//			File prevDelete = new File(prevRevisionDir, docDeleteSetName);
//			//이전 리비전의 delete.set을 현재 리비전 dir로 가져와서 이어쓰도록 한다.
//			FileUtils.copyFileToDirectory(prevDelete, revisionDir);
//			deleteSet = new BitSet(segmentDir, docDeleteSetName);
//		} else {
//			deleteSet = new BitSet(segmentDir, docDeleteSetName, true);
//		}
	}

	public int getUpdateDocCount() {
		return updateDocCount;
	}
	
	public void write(Document document, int localDocNo) throws IOException, IRException {
		
		if(hasPrimaryKey){
			pkbaos.reset();
			for (int fieldId : primaryKeyFieldIdList) {
				Field f = document.get(fieldId);
				if(f == null || f.isNull()){
					throw new IOException("PK field value cannot be null. fieldId="+fieldId+", field="+f+", localDocNo="+localDocNo);
				} else {
					f.writeFixedDataTo(pkbaos);
				}
//				logger.debug("PK >> {}", f);
			}
			
			int preDocNo = indexWriter.put(pkbaos.array(), 0, (int) pkbaos.position(), localDocNo);
			
			if (preDocNo >= 0) {
				if(logger.isDebugEnabled()) {
					logger.debug("PK updated! >> newDocNo[{}] docNo[{}] pk[{}]", localDocNo, preDocNo, new String(pkbaos.array(), 0, (int) pkbaos.position()));
				}
				deleteSet.set(preDocNo);
				updateDocCount++;// 수집시 데이터내에 서로 중복된 문서가 발견된 경우 count증가.
			}
			
			count++;
			if (count % CHECK_COUNT == 0) {
				long memorySize = indexWriter.checkWorkingMemorySize();
				logger.info("PK check #{} pk mem {}", count, Formatter.getFormatSize(memorySize));
				if (memorySize > MEMORY_LIMIT) {
					indexWriter.flush();
				}
			}
			
			
		}
	}

	public void close() throws IOException {
		if(indexWriter != null){
			indexWriter.close();
		}
		
		// save delete list
		deleteSet.save();
	}

}
