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
import java.util.List;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.document.PrimaryKeyIndexReader;
import org.fastcatsearch.ir.field.Field;
import org.fastcatsearch.ir.field.FieldDataParseException;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.ir.io.CharVector;
import org.fastcatsearch.ir.io.DataOutput;
import org.fastcatsearch.ir.query.Term;
import org.fastcatsearch.ir.search.clause.OperatedClause;
import org.fastcatsearch.ir.search.clause.TermOperatedClause;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.fastcatsearch.ir.settings.PrimaryKeySetting;
import org.fastcatsearch.ir.settings.RefSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * */
public class PrimaryKeyIndexesReader implements Cloneable {
	private static Logger logger = LoggerFactory.getLogger(PrimaryKeyIndexesReader.class);

	private FieldSetting[] pkFieldSettingList;

	private PrimaryKeyIndexReader pkReader;
	
	public PrimaryKeyIndexesReader() {
	}

	public PrimaryKeyIndexesReader(Schema schema, File dir) throws IOException, IRException {
		PrimaryKeySetting primaryKeySetting = schema.schemaSetting().getPrimaryKeySetting();
		List<RefSetting> pkRefSettingList = primaryKeySetting.getFieldList();
		pkFieldSettingList = new FieldSetting[pkRefSettingList.size()];

		for (int i = 0; i < pkRefSettingList.size(); i++) {
			String refId = pkRefSettingList.get(i).getRef();
			pkFieldSettingList[i] = schema.getFieldSetting(refId);
		}

		pkReader = new PrimaryKeyIndexReader(dir, IndexFileNames.primaryKeyMap);
	}

	@Override
	public PrimaryKeyIndexesReader clone() {

		PrimaryKeyIndexesReader reader = new PrimaryKeyIndexesReader();
		reader.pkFieldSettingList = pkFieldSettingList;
		reader.pkReader = pkReader.clone();
		return reader;
	}

	public OperatedClause getOperatedClause(Term term) throws IOException, FieldDataParseException {
		int weight = term.weight();
		String termString = term.termString();
		String[] list = termString.split(" ");
		int size = 1;
		if (list.length > 1) {
			size = list.length;
		}
		// 중복된 아이디리스트가 입력될수 있으므로 정렬후 중복제거 수행
		quickSort(list, 0, list.length - 1);
		size = removeRedundancy(list, list.length);
		logger.debug("search primary key! size={}", size);

		PostingDoc[] termDocList = new PostingDoc[size];
		int m = 0;
		BytesDataOutput pkOutput = new BytesDataOutput();

		String[] pkValues = null;
		for (int i = 0; i < size; i++) {

			pkOutput.reset();

			String pkValue = list[i];

			if (pkFieldSettingList.length > 1) {
				// 결합 pk일경우 값들은 ';'로 구분되어있다.
				pkValues = pkValue.split(";");
			} else {
				pkValues = new String[] { pkValue };
			}

			int docNo = -1;

			for (int j = 0; j < pkFieldSettingList.length; j++) {
				FieldSetting fieldSetting = pkFieldSettingList[j];
				Field field = fieldSetting.createIndexableField(pkValues[j]);
				field.writeTo(pkOutput);
			}

			docNo = pkReader.get(pkOutput.array(), 0, (int) pkOutput.position());
			if (docNo != -1) {
				termDocList[m] = new PostingDoc(docNo, 1);
				m++;
			}
		}

		OperatedClause idOperatedClause = null;
		if (m > 0) {
			idOperatedClause = new TermOperatedClause("_PK", "", new DataPostingReader(new CharVector(termString), 0, weight, termDocList, m));
		}

		return idOperatedClause;

	}

	public int getDocNo(String pkValue, BytesDataOutput pkOutput) throws FieldDataParseException, IOException {
		pkOutput.reset();
		String[] pkValues = null;
		if (pkFieldSettingList.length > 1) {
			// 결합 pk일경우 값들은 ';'로 구분되어있다.
			pkValues = pkValue.split(";");
		} else {
			pkValues = new String[] { pkValue };
		}
		for (int j = 0; j < pkFieldSettingList.length; j++) {
			FieldSetting fieldSetting = pkFieldSettingList[j];
			Field field = fieldSetting.createIndexableField(pkValues[j]);
			field.writeFixedDataTo(pkOutput);
		}

		return pkReader.get(pkOutput.array(), 0, (int) pkOutput.position());
	}
	
	
	public void close() throws IOException {
		pkReader.close();
	}
	
	private void quickSort(String[] e, int first, int last) {

		if (last <= 0)
			return;

		int stackMaxSize = (int) ((Math.log(last - first + 1) + 3) * 2);
		int[][] stack = new int[stackMaxSize][2];

		String pivotId = null;
		int sp = 0;
		int left = 0, right = 0;

		while (true) {
			while (first < last) {
				left = first;
				right = last;
				int median = (left + right) / 2;

				// move pivot to left most.
				String tmp = e[left];
				e[left] = e[median];
				e[median] = tmp;
				pivotId = e[left];

				while (left < right) {
					while (e[right].compareTo(pivotId) >= 0 && (left < right))
						right--;

					if (left != right) {
						e[left] = e[right];
						left++;
					}

					while (e[left].compareTo(pivotId) <= 0 && (left < right))
						left++;

					if (left != right) {
						e[right] = e[left];
						right--;
					}
				}

				e[left] = pivotId;

				if (left - first < last - left) {
					if (left + 1 < last) {
						sp++;
						stack[sp][0] = left + 1;
						stack[sp][1] = last;
					}
					last = left - 1;
				} else {
					if (first < left - 1) {
						sp++;
						stack[sp][0] = first;
						stack[sp][1] = left - 1;
					}
					first = left + 1;
				}

			}

			if (sp == 0) {
				return;
			} else {
				first = stack[sp][0];
				last = stack[sp][1];
				sp--;
			}

		}
	}

	private int removeRedundancy(String[] list, int length) {
		String prev = "";
		int c = 0;
		for (int i = 0; i < length; i++) {
			list[i] = list[i].trim();
			String str = list[i];
			if (!str.equalsIgnoreCase(prev) && prev.length() > 0) {
				list[c++] = prev;
			}
			prev = str;
		}
		if (prev.length() > 0)
			list[c++] = prev;

		return c;
	}
}
