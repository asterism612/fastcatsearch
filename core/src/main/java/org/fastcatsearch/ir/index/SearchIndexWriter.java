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

import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.AnalyzerOption;
import org.apache.lucene.analysis.tokenattributes.AdditionalTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharsRefTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.StopwordAttribute;
import org.apache.lucene.util.CharsRef;
import org.fastcatsearch.ir.analysis.AnalyzerPool;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.field.Field;
import org.fastcatsearch.ir.index.temp.TempSearchFieldMerger;
import org.fastcatsearch.ir.io.BufferedFileOutput;
import org.fastcatsearch.ir.io.CharVector;
import org.fastcatsearch.ir.io.IndexOutput;
import org.fastcatsearch.ir.settings.IndexRefSetting;
import org.fastcatsearch.ir.settings.IndexSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchIndexWriter implements SingleIndexWriter {
	private static Logger logger = LoggerFactory.getLogger(SearchIndexWriter.class);
	
	private String indexId;
	private MemoryPosting memoryPosting;
	private IndexFieldOption fieldIndexOption;
	private AnalyzerPool[] indexAnalyzerPoolList;
	private Analyzer[] indexAnalyzerList;
	private File baseDir;

	private boolean ignoreCase;
	private IndexConfig indexConfig;

	private File tempFile;
	private IndexOutput tempOutput;
	private List<Long> flushPosition; // each flush file position
	private int count;
	private int[] indexFieldSequence; // index내에 색인할 필드가 여러개일 경우 필드 번호.
	private int positionIncrementGap;

	private SegmentInfo segmentInfo;
	private AnalyzerOption indexingAnalyzerOption;
	
	@Override
	public String toString(){
		return indexId;
	}
	public SearchIndexWriter(IndexSetting indexSetting, Schema schema, File dir, SegmentInfo segmentInfo, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager) throws IOException,
			IRException {
		this.indexId = indexSetting.getId();
		this.baseDir = dir;
		this.segmentInfo = segmentInfo;
		this.indexConfig = indexConfig;
		
		ignoreCase = indexSetting.isIgnoreCase();
		int indexBucketSize = indexConfig.getIndexWorkBucketSize();

		fieldIndexOption = new IndexFieldOption();
		if (indexSetting.isStorePosition()) {
			memoryPosting = new MemoryPostingWithPosition(indexBucketSize, ignoreCase);
			fieldIndexOption.setStorePosition();
		} else {
			memoryPosting = new MemoryPosting(indexBucketSize, ignoreCase);
		}

		List<IndexRefSetting> refList = indexSetting.getFieldList();
		indexFieldSequence = new int[refList.size()];
		indexAnalyzerPoolList = new AnalyzerPool[refList.size()];
		indexAnalyzerList = new Analyzer[refList.size()];
		
		for (int i = 0; i < refList.size(); i++) {
			IndexRefSetting refSetting = refList.get(i);
			String fieldId = refSetting.getRef();
			String indexAnalyzerId = refSetting.getIndexAnalyzer();
			
			AnalyzerPool analyzerPool = analyzerPoolManager.getPool(indexAnalyzerId);

			if (analyzerPool == null) {
				// 분석기 못찾음.
				throw new IRException("분석기를 찾을 수 없습니다. " + indexAnalyzerId);
			}
			
			indexFieldSequence[i] = schema.getFieldSequence(fieldId);
			indexAnalyzerPoolList[i] = analyzerPool;
			indexAnalyzerList[i] = analyzerPool.getFromPool();
		}
		
		positionIncrementGap = indexSetting.getPositionIncrementGap();

		flushPosition = new ArrayList<Long>();

		tempFile = new File(dir, IndexFileNames.getSearchTempFileName(indexId));
		tempOutput = new BufferedFileOutput(tempFile, false);
		
		//색인시는 stopword만 본다.
		indexingAnalyzerOption = new AnalyzerOption();
		indexingAnalyzerOption.useStopword(true);
		indexingAnalyzerOption.setForDocument();
	}

	public void write(Document doc) throws IRException, IOException {
		write(doc, count);
	}

	@Override
	public void write(Document doc, int docNo) throws IRException, IOException {

		int[] sequenceList = indexFieldSequence;
		for (int i = 0; i < sequenceList.length; i++) {
			int sequence = sequenceList[i];
			if(sequence < 0){
				continue;
			}
			write(docNo, i, doc.get(sequence), ignoreCase, positionIncrementGap);
			// positionIncrementGap은 필드가 증가할때마다 동일량으로 증가. 예) 0, 100, 200, 300...
			positionIncrementGap += positionIncrementGap;
		}

		count++;
	}

	private void write(int docNo, int i, Field field, boolean isIgnoreCase, int positionIncrementGap) throws IRException, IOException {
		if (field == null) {
			return;
		}

		// 같은문서에 indexFieldNum가 중복되어서 들어오면 multi-field-index로 처리한다.
		if (field.isMultiValue()) {
			Iterator<Object> iterator = field.getMultiValueIterator();
			if (iterator != null) {
				while (iterator.hasNext()) {
					indexValue(docNo, i, iterator.next(), isIgnoreCase, positionIncrementGap);
					// 멀티밸류도 positionIncrementGap을 증가시킨다. 즉, 필드가 다를때처럼 position거리가 멀어진다.
					positionIncrementGap += positionIncrementGap;
				}
			}
		} else {
			indexValue(docNo, i, field.getValue(), isIgnoreCase, positionIncrementGap);
		}
	}

	private void indexValue(int docNo, int i, Object value, boolean isIgnoreCase, int positionIncrementGap) throws IOException, IRException {
		if(value == null){
			return;
		}
		char[] fieldValue = value.toString().toCharArray();
		TokenStream tokenStream = indexAnalyzerList[i].tokenStream(indexId, new CharArrayReader(fieldValue), indexingAnalyzerOption);
		tokenStream.reset();
		CharsRefTermAttribute termAttribute = null;
		PositionIncrementAttribute positionAttribute = null;
		StopwordAttribute stopwordAttribute = null;
		AdditionalTermAttribute additionalTermAttribute = null;
		CharTermAttribute charTermAttribute = null;
		//색인시는 유사어확장을 하지 않는다.
		
		if (tokenStream.hasAttribute(CharsRefTermAttribute.class)) {
			termAttribute = tokenStream.getAttribute(CharsRefTermAttribute.class);
		}
		if (tokenStream.hasAttribute(PositionIncrementAttribute.class)) {
			positionAttribute = tokenStream.getAttribute(PositionIncrementAttribute.class);
		}
		if (tokenStream.hasAttribute(AdditionalTermAttribute.class)) {
			additionalTermAttribute = tokenStream.getAttribute(AdditionalTermAttribute.class);
		}
		
		// stopword 처리.
		if (tokenStream.hasAttribute(StopwordAttribute.class)) {
			stopwordAttribute = tokenStream.getAttribute(StopwordAttribute.class);
		}
		if (tokenStream.hasAttribute(CharTermAttribute.class)) {
			charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
		}
		
		int lastPosition = 0;
	
		while (tokenStream.incrementToken()) {
			CharVector key = null;
			if (termAttribute != null) {
				CharsRef charRef = termAttribute.charsRef();
				char[] buffer = new char[charRef.length()];
				System.arraycopy(charRef.chars, charRef.offset, buffer, 0, charRef.length);
				key = new CharVector(buffer, 0, buffer.length);
			} else {
				key = new CharVector(charTermAttribute.buffer(), 0, charTermAttribute.length());
			}
			
			int position = -1;
			if (positionAttribute != null) {
				position = positionAttribute.getPositionIncrement() + positionIncrementGap;
				lastPosition = position;
			}
//			logger.debug("FIELD#{}: {} >> {} ({})", indexId, key, docNo, position);
			if(stopwordAttribute != null && stopwordAttribute.isStopword()){
				//ignore
			}else{
				memoryPosting.add(key, docNo, position);
			}
//			if(synonymAttribute != null) {
//				CharVector[] synonym = synonymAttribute.getSynonym();
//				if(synonym != null) {
//					for(CharVector token : synonym) {
//						memoryPosting.add(token, docNo, position);
//					}
//				}
//			}
			if(additionalTermAttribute!=null && additionalTermAttribute.size() > 0) {
				Iterator<String> iter = additionalTermAttribute.iterateAdditionalTerms();
				while(iter.hasNext()) {
					CharVector token = new CharVector(iter.next().toCharArray());
					memoryPosting.add(token, docNo, lastPosition);
				}
			}
		}
	}

	public int checkWorkingMemorySize() {
		return memoryPosting.workingMemorySize();
	}

	public int checkStaticMemorySize() {
		return memoryPosting.staticMemorySize();
	}

	public int checkTotalCount() {
		return memoryPosting.count();
	}

	public void flush() throws IRException {
		if (count <= 0) {
			return;
		}

		logger.info("[{}] Flush#{} [documents {}th..]", indexId, flushPosition.size() + 1, count);

		try {
			flushPosition.add(memoryPosting.save(tempOutput));
			// ensure every data wrote on disk!
			tempOutput.flush();

			memoryPosting.clear();
		} catch (IOException e) {
			throw new IRException(e);
		}
	}

	public void close() throws IRException, IOException {

		// Analyzer 리턴.
		for (int i = 0; i < indexAnalyzerPoolList.length; i++) {
			if(indexAnalyzerPoolList[i] != null && indexAnalyzerList[i] != null){
				indexAnalyzerPoolList[i].releaseToPool(indexAnalyzerList[i]);
			}
		}
		
		try {
			flush();
		} finally {
			tempOutput.close();
		}

		try {
			if (count > 0) {
				logger.debug("Close, flushCount={}", flushPosition.size());

//				if (revisionInfo.isAppend()) {
//					File prevAppendDir = IndexFileNames.getRevisionDir(baseDir, revisionInfo.getRef());
//					File revisionDir = IndexFileNames.getRevisionDir(baseDir, revisionInfo.getId());
//					TempSearchFieldAppender appender = new TempSearchFieldAppender(indexId, flushPosition, tempFile);
//					try {
//						appender.mergeAndAppendIndex(prevAppendDir, revisionDir, indexConfig.getIndexTermInterval(), fieldIndexOption);
//					} finally {
//						appender.close();
//					}
//				} else {
					TempSearchFieldMerger merger = new TempSearchFieldMerger(indexId, flushPosition, tempFile);
					try {
						merger.mergeAndMakeIndex(baseDir, indexConfig.getIndexTermInterval(), fieldIndexOption);
					} finally {
						merger.close();
					}
//				}
			}
		} finally {
			// delete temp file
			tempFile.delete();
		}
	}

}
