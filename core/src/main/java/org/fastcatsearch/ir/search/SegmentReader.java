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

import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.document.DocumentReader;
import org.fastcatsearch.ir.io.BitSet;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.util.CloseableThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton, Thread-safe 하다. 각 세그먼트에서 검색을 하고 필터링, 그룹핑을 수행한 결과 문서리스트를 리턴한다.
 * 
 * 각 세그먼트에서는 Hit문서번호, 랭크데이터를 제공해주어야 한다.
 * 
 * @author sangwook.song
 * 
 */
public class SegmentReader {

	private static final Logger logger = LoggerFactory.getLogger(SegmentReader.class);

	private int segmentSequence;
	private Schema schema;
	private SearchIndexesReader searchIndexesReader;
	private FieldIndexesReader fieldIndexesReader;
	private GroupIndexesReader groupIndexesReader;
	private DocumentReader documentReader;
	private BitSet deleteSet;
	private SegmentInfo segmentInfo;
	private File segmentDir;

	final CloseableThreadLocal<DocumentReader> documentReaderLocal = new CloseableThreadLocal<DocumentReader>() {
		@Override
		protected DocumentReader initialValue() {
			return documentReader.clone();
		}
	};
	
	final CloseableThreadLocal<SearchIndexesReader> searchIndexesReaderLocal = new CloseableThreadLocal<SearchIndexesReader>() {
		@Override
		protected SearchIndexesReader initialValue() {
			return searchIndexesReader.clone();
		}
	};
	
	final CloseableThreadLocal<FieldIndexesReader> fieldIndexesReaderLocal = new CloseableThreadLocal<FieldIndexesReader>() {
		@Override
		protected FieldIndexesReader initialValue() {
			return fieldIndexesReader.clone();
		}
	};
	
	final CloseableThreadLocal<GroupIndexesReader> groupIndexesReaderLocal = new CloseableThreadLocal<GroupIndexesReader>() {
		@Override
		protected GroupIndexesReader initialValue() {
			return groupIndexesReader.clone();
		}
	};
	
	public SegmentReader(SegmentInfo segmentInfo, Schema schema, File segmentDir, AnalyzerPoolManager analyzerPoolManager) throws IOException, IRException {
		this(segmentInfo, schema, segmentDir, null, analyzerPoolManager);
	}
			
	public SegmentReader(SegmentInfo segmentInfo, Schema schema, File segmentDir, BitSet bitset, AnalyzerPoolManager analyzerPoolManager) throws IOException, IRException {
		this.segmentSequence = segmentInfo.getIntId();
		this.schema = schema;
		this.segmentDir = segmentDir;
		this.segmentInfo = segmentInfo;
//		int revision = segmentInfo.getRevisionInfo().getId();
//		int ref = segmentInfo.getRevisionInfo().getRef();
		
		this.documentReader = new DocumentReader(schema.schemaSetting(), segmentDir, segmentInfo.getBaseNumber());
		int documentCount = documentReader.getDocumentCount();
		
		// reader들은 thread-safe하지 않다. clone해서 사용됨.
		this.searchIndexesReader = new SearchIndexesReader(schema, segmentDir, analyzerPoolManager, documentCount);
		
		//field index
		this.fieldIndexesReader = new FieldIndexesReader(schema, segmentDir);
		
//		// group index
		this.groupIndexesReader = new GroupIndexesReader(schema, segmentDir);

		
		
		if (bitset != null) {
			deleteSet = bitset;
		} else {
//			deleteSet = new BitSet(IndexFileNames.getRevisionDir(segmentDir, revision), IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentInfo.getId()));
//			deleteSet = new BitSet(segmentDir, IndexFileNames.getSuffixFileName(IndexFileNames.docDeleteSet, segmentInfo.getId()));
			deleteSet = new BitSet(segmentDir, IndexFileNames.docDeleteSet);
		}
	}

	public SegmentSearcher segmentSearcher(){
		return new SegmentSearcher(this);
	}
	
	protected int sequence(){
		return segmentSequence;
	}
	
	protected Schema schema(){
		return schema;
	}
	
	public File segmentDir(){
		return segmentDir;
	}
	
//	public File revisionDir(){
//		return new File(segmentDir, Integer.toString(segmentInfo.getRevision()));
//	}
	
	public SegmentInfo segmentInfo(){
		return segmentInfo;
	}
	
	protected int docCount(){
		return documentReader.getDocumentCount();
	}
	
	protected int baseDocNumber(){
		return documentReader.getBaseNumber();
	}
	
	public BitSet deleteSet(){
		return deleteSet;
	}
	
	public SearchIndexesReader newSearchIndexesReader(){
		return searchIndexesReaderLocal.get();
	}
	
	public FieldIndexesReader newFieldIndexesReader(){
		return fieldIndexesReaderLocal.get();
	}
	
	public GroupIndexesReader newGroupIndexesReader(){
		return groupIndexesReaderLocal.get();
	}
	
	public DocumentReader newDocumentReader(){
		return documentReaderLocal.get();
	}
	
	public void setDeleteSet(BitSet deleteSet) {
		this.deleteSet = deleteSet;
	}

	public void close() throws IOException {
		IOException exception = null; 
		try{
			searchIndexesReader.close();
		}catch(IOException e){ exception = e; }
		
		try{
			fieldIndexesReader.close();
		}catch(IOException e){ exception = e; }
		
		try{
			groupIndexesReader.close();
		}catch(IOException e){ exception = e; }
		
		try{
			documentReader.close();
		}catch(IOException e){ exception = e; }
		
		if(exception != null){
			throw exception;
		}
	}
}
