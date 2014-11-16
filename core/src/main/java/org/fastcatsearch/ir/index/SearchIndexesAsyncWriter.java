///*
// * Copyright 2013 Websquared, Inc.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *   http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.fastcatsearch.ir.index;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//
//import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
//import org.fastcatsearch.ir.common.IRException;
//import org.fastcatsearch.ir.config.DataInfo.RevisionInfo;
//import org.fastcatsearch.ir.config.IndexConfig;
//import org.fastcatsearch.ir.document.Document;
//import org.fastcatsearch.ir.index.async.IndexWriteTask;
//import org.fastcatsearch.ir.index.async.IndexWriteTaskPoolWriter;
//import org.fastcatsearch.ir.settings.IndexSetting;
//import org.fastcatsearch.ir.settings.Schema;
//import org.fastcatsearch.ir.util.Formatter;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class SearchIndexesAsyncWriter {
//	private static Logger logger = LoggerFactory.getLogger(SearchIndexesAsyncWriter.class);
//
//	private List<IndexSetting> indexSettingList;
//	private SearchIndexWriter[] searchIndexWriterList;
//	IndexWriteTaskPoolWriter poolWriter;
//	
//	private int indexSize;
//	
//	// limit memory use. if exeed this value, flush.
//	private long workMemoryLimit;
//	private int workMemoryCheck = 10000; //해당 갯수만큼 색인문서가 진행되면 정보를 출력한다. 
//
//	private int count;
//
//	public SearchIndexesAsyncWriter(Schema schema, File dir, RevisionInfo revisionInfo, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager, BlockingQueue<IndexWriteTask> taskQueue) throws IOException, IRException {
//		this(schema, dir, revisionInfo, indexConfig, analyzerPoolManager, taskQueue, null);
//	}
//
//	public SearchIndexesAsyncWriter(Schema schema, File dir, RevisionInfo revisionInfo, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager,
//			BlockingQueue<IndexWriteTask> taskQueue, List<String> indexIdList) throws IOException, IRException {
//		this.indexSettingList = schema.schemaSetting().getIndexSettingList();
//		
//		int totalSize = indexSettingList == null ? 0 : indexSettingList.size();
//		
//		List<SearchIndexWriter> list = new ArrayList<SearchIndexWriter>();
//
//		for (int i = 0; i < totalSize; i++) {
//			IndexSetting indexSetting = indexSettingList.get(i);
//			if(indexIdList == null || indexIdList.contains(indexSetting.getId())){
//				SearchIndexWriter searchIndexWriter = new SearchIndexWriter(indexSetting, schema, dir, revisionInfo, indexConfig, analyzerPoolManager);
//				list.add(searchIndexWriter);
//			}
//		}
//		searchIndexWriterList = list.toArray(new SearchIndexWriter[0]);
//		indexSize = searchIndexWriterList.length;
//		
//		workMemoryLimit = indexConfig.getIndexWorkMemorySize();
//		poolWriter = new IndexWriteTaskPoolWriter(taskQueue, searchIndexWriterList);
//	}
//
//	public void write(Document doc) throws IRException, IOException {
//		write(doc, count);
//	}
//
//	
//	public void write(Document doc, int docNo) throws IRException, IOException {
////		for (int i = 0; i < indexSize; i++) {
////			searchIndexWriterList[i].write(doc, docNo);
////		}
////		logger.debug("#######write > {}", docNo);
//		poolWriter.write(doc, docNo);
////		logger.debug("#######write done> {}", docNo);
//		if ((count + 1) % workMemoryCheck == 0) {
//			int workingMemorySize = checkWorkingMemorySize();
//			logger.debug("SearchField Memory = {}, limit = {}", Formatter.getFormatSize(workingMemorySize), Formatter.getFormatSize(workMemoryLimit));
//			if (workingMemorySize > workMemoryLimit) {
//				logger.info("Write memory used = {}", count + 1, workingMemorySize);
//				flush();
//			}
//		}else if ((count + 1) % 1000 == 0) {
//			//1000개씩 확인해본다.
//			int workingMemorySize = checkWorkingMemorySize();
//			if (workingMemorySize > workMemoryLimit) {
//				logger.info("[{}] documents write memory used = {}", count + 1, workingMemorySize);
//				flush();
//			}
//		}
//		count++;
////		logger.debug("#######write end> {}, count = {}", docNo, count);
//	}
//
////	public boolean writeJoin() {
////		
////	}
//	
//	private int checkWorkingMemorySize() {
//		int totalMemorySize = 0;
//		for (int i = 0; i < indexSize; i++) {
//			totalMemorySize += searchIndexWriterList[i].checkWorkingMemorySize();
//		}
////		logger.debug("SearchIndex Working Mem[{}]", Formatter.getFormatSize(totalMemorySize));
//		return totalMemorySize;
//	}
//
//	private int checkStaticMemorySize() {
//		int totalMemorySize = 0;
//		for (int i = 0; i < indexSize; i++) {
//			totalMemorySize += searchIndexWriterList[i].checkStaticMemorySize();
//		}
//		logger.debug("SearchIndex Static Mem[{}]", Formatter.getFormatSize(totalMemorySize));
//		return totalMemorySize;
//	}
//
//	private int checkTotalCount() {
//		int totalCount = 0;
//		for (int i = 0; i < indexSize; i++) {
//			totalCount += searchIndexWriterList[i].checkTotalCount();
//		}
//		logger.debug("SearchIndex term count[{}]", totalCount);
//		return totalCount;
//	}
//
//
//
//	public void flush() throws IRException {
//		if (count <= 0) {
//			return;
//		}
//		
//		for (int i = 0; i < indexSize; i++) {
//			searchIndexWriterList[i].flush();
//		}
//	}
//
//	public void close() throws IRException, IOException {
//		
//		for (int i = 0; i < indexSize; i++) {
//			if(searchIndexWriterList[i] != null){
//				searchIndexWriterList[i].close();
//			}
//		}
//	
//	}
//
//}
