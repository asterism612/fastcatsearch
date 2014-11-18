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
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.ir.analysis.AnalyzerPool;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.query.HighlightInfo;
import org.fastcatsearch.ir.query.Term;
import org.fastcatsearch.ir.search.clause.OperatedClause;
import org.fastcatsearch.ir.search.clause.OrOperatedClause;
import org.fastcatsearch.ir.settings.IndexSetting;
import org.fastcatsearch.ir.settings.PrimaryKeySetting;
import org.fastcatsearch.ir.settings.RefSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 검색시 numeric field도 모두 string 형을 처리하기때문에, key는 123 1200 2 20000 31 과 같이 정렬되어 있다.
 * */
public class SearchIndexesReader implements Cloneable {
	private static Logger logger = LoggerFactory.getLogger(SearchIndexesReader.class);

	private Schema schema;
	private List<IndexSetting> indexSettingList;
	private ArrayList<SearchIndexReader> readerList;
	private PrimaryKeyIndexesReader primaryKeyIndexesReader;
	private int segmentDocumentCount;

	public SearchIndexesReader() {
	}

	public SearchIndexesReader(Schema schema, File dir, AnalyzerPoolManager analyzerPoolManager, int segmentDocumentCount) throws IOException, IRException {
		this.schema = schema;
		this.segmentDocumentCount = segmentDocumentCount;
//		logger.debug("schema > {}", schema);
//		logger.debug("schema.schemaSetting > {}", schema.schemaSetting());
		indexSettingList = schema.schemaSetting().getIndexSettingList();
		int indexCount = indexSettingList == null ? 0 : indexSettingList.size();

		// 색인파일열기.
		readerList = new ArrayList<SearchIndexReader>(indexCount);
		for (int i = 0; i < indexCount; i++) {
			IndexSetting setting = indexSettingList.get(i);
			SearchIndexReader reader = null;
			try {
				String queryAnalyzerName = setting.getQueryAnalyzer();
				AnalyzerPool queryAnalyzerPool = analyzerPoolManager.getPool(queryAnalyzerName);
				
				if (queryAnalyzerPool != null) {
					logger.debug("[{}] QueryTokenizer={}", setting.getId(), queryAnalyzerPool.getClass().getSimpleName());
				} else {
					// 분석기를 못찾았을 경우.
					throw new IRException("Query analyzer not found >> " + setting.getId() + " : " + queryAnalyzerName);
				}
				
				reader = new SearchIndexReader(setting, schema, dir, queryAnalyzerPool, segmentDocumentCount);
			} catch (Exception e) {
				logger.error("색인Reader {}로딩중 에러 >> {}", setting.getId(), e);
				if (reader != null) {
					reader.close();
				}
			}
			readerList.add(reader);
		}
		PrimaryKeySetting primaryKeySetting = schema.schemaSetting().getPrimaryKeySetting();
		if(primaryKeySetting.getFieldList() != null && primaryKeySetting.getFieldList().size() > 0) {
			primaryKeyIndexesReader = new PrimaryKeyIndexesReader(schema, dir);
		}
	}

	@Override
	public SearchIndexesReader clone() {

		SearchIndexesReader reader = new SearchIndexesReader();
		reader.schema = schema;
		reader.segmentDocumentCount = segmentDocumentCount;
		reader.indexSettingList = indexSettingList;
		reader.readerList = new ArrayList<SearchIndexReader>(readerList.size());
//		logger.debug("clone readerList.size > {}",readerList.size());
		for (SearchIndexReader r : readerList) {
			SearchIndexReader newReader = null;
			if (r != null) {
				newReader = r.clone();
			}
			if(newReader != null){
				reader.readerList.add(newReader);
			}
		}
		if(primaryKeyIndexesReader != null) {
			reader.primaryKeyIndexesReader = primaryKeyIndexesReader.clone();
		}
		
		return reader;
	}
	
	public OperatedClause getOperatedClause(Term term, HighlightInfo highlightInfo) throws IOException, IRException {
		String[] indexFieldIdList = term.indexFieldId();

		OperatedClause totalClause = null;
		for (int i = 0; i < indexFieldIdList.length; i++) {
			String indexFieldId = indexFieldIdList[i];
			logger.debug("getOperatedClause [{}] >> [{}]", indexFieldId, term);

			boolean isPrimaryKeyField = false;
			int indexFieldSequence = schema.getSearchIndexSequence(indexFieldId);
			logger.debug("getSearchIndexSequence {} > {}", indexFieldId, indexFieldSequence);
			if (indexFieldSequence < 0) {
				String primaryKeyId = schema.schemaSetting().getPrimaryKeySetting().getId();
				logger.debug("getSearchIndexSequence primaryKeyId > {}", primaryKeyId);
				if (indexFieldId.equals(primaryKeyId)) {
					isPrimaryKeyField = true;
				} else {
					throw new IRException("Unknown Search index field id \"" + indexFieldId + "\"");
				}
			}

			OperatedClause oneFieldClause = null;
			if (isPrimaryKeyField) {
				if(primaryKeyIndexesReader != null) {
					oneFieldClause = primaryKeyIndexesReader.getOperatedClause(term);
				}else{
					continue;
				}
			} else {
				SearchIndexReader searchIndexReader = readerList.get(indexFieldSequence);
				oneFieldClause = term.createOperatedClause(searchIndexReader, highlightInfo);
			}

			if (totalClause == null) {
				totalClause = oneFieldClause;
			} else {
				totalClause = new OrOperatedClause(totalClause, oneFieldClause);
			}

		}// for

		return totalClause;

	}
	
	public PrimaryKeyIndexesReader getPrimaryKeyIndexesReader() {
		return primaryKeyIndexesReader; 
	}
	
	public SearchIndexReader getSearchIndexReader(String indexFieldId) throws IOException, IRException {
		int indexFieldSequence = schema.getSearchIndexSequence(indexFieldId);
		logger.debug("getSearchIndexSequence {} > {}", indexFieldId, indexFieldSequence);
		if (indexFieldSequence < 0) {
			return null;
		}
		return readerList.get(indexFieldSequence);
	}

	public void close() throws IOException {
		if (readerList != null) {
			for (SearchIndexReader reader : readerList) {
				if (reader != null) {
					reader.close();
				}
			}
		}
		if(primaryKeyIndexesReader != null) {
			primaryKeyIndexesReader.close();
		}
	}
}
