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

import org.apache.lucene.analysis.Analyzer;
import org.fastcatsearch.ir.analysis.AnalyzerPool;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.index.IndexFieldOption;
import org.fastcatsearch.ir.io.BufferedFileInput;
import org.fastcatsearch.ir.io.CharVector;
import org.fastcatsearch.ir.io.FixedMinHeap;
import org.fastcatsearch.ir.io.IOUtil;
import org.fastcatsearch.ir.io.IndexInput;
import org.fastcatsearch.ir.search.method.AbstractSearchMethod;
import org.fastcatsearch.ir.search.method.SearchMethod;
import org.fastcatsearch.ir.search.posting.PostingDocsMerger;
import org.fastcatsearch.ir.search.posting.PostingDocsReader;
import org.fastcatsearch.ir.settings.IndexSetting;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 검색시 numeric field도 모두 string 형으로 처리하기때문에, key는 123 1200 2 20000 31 과 같이 정렬되어 있다.
 * */
public class SearchIndexReader implements Cloneable {
	private static Logger logger = LoggerFactory.getLogger(SearchIndexReader.class);

	private String indexId;
	private IndexInput postingInput;
	private IndexInput lexiconInput;
	private Schema schema;
	private MemoryLexicon memoryLexicon;
	private long fileLimit;
	private IndexFieldOption indexFieldOption;

	private AnalyzerPool queryAnalyzerPool;
	private IndexSetting indexSetting;
	private int segmentDocumentCount;
	
	public SearchIndexReader() {
	}

	public SearchIndexReader(IndexSetting indexSetting, Schema schema, File dir, AnalyzerPool queryAnalyzerPool, int segmentDocumentCount) throws IOException, IRException {
		this(indexSetting, schema, dir, 0, queryAnalyzerPool, segmentDocumentCount);
	}

	public SearchIndexReader(IndexSetting indexSetting, Schema schema, File dir, int revision, AnalyzerPool queryAnalyzerPool, int segmentDocumentCount) throws IOException, IRException {
		this.schema = schema;
		this.indexSetting = indexSetting;
		String id = indexSetting.getId();
		this.indexId = id;
		this.queryAnalyzerPool = queryAnalyzerPool;
		this.segmentDocumentCount = segmentDocumentCount;
		
		logger.debug("Search Index [{}] Dir = {}", indexId, dir.getAbsolutePath());
		try {
			postingInput = new BufferedFileInput(dir , IndexFileNames.getSearchPostingFileName(id));
			lexiconInput = new BufferedFileInput(dir , IndexFileNames.getSearchLexiconFileName(id));
			
			fileLimit = lexiconInput.length();
			
			// posting 파일의 첫 int는 색인옵션.
			indexFieldOption = new IndexFieldOption(postingInput.readInt());

		} catch (Exception e) {
			if (postingInput != null) {
				postingInput.close();
			}
			if (lexiconInput != null) {
				lexiconInput.close();
			}
			throw new IRException(e);
		}
		
		IndexInput indexInput = null;
		try {
			indexInput = new BufferedFileInput(dir, IndexFileNames.getSearchIndexFileName(id));
			int indexSize = indexInput.readInt();

			logger.debug("====memoryLexicon - {}==== index key size = {}", id, indexSize);
			if (indexSize > 0) {
				memoryLexicon = new MemoryLexicon(indexSize);
				// Load lexicon-index to memory
				for (int k = 0; k < indexSize; k++) {
					memoryLexicon.put(k, indexInput.readUString(), indexInput.readLong(), indexInput.readLong());
				}
			} else {
				memoryLexicon = new MemoryLexicon(0);
			}
		} finally {
			if (indexInput != null) {
				indexInput.close();
			}
		}
		
		
	}

	public String indexId(){
		return indexId;
	}
	public void close() throws IOException {
		lexiconInput.close();
		postingInput.close();
	}
	
	
	@Override
	public SearchIndexReader clone() {
		
		SearchIndexReader reader = new SearchIndexReader();
		reader.indexId = indexId;
		reader.schema = schema;
		reader.postingInput = postingInput.clone();
		reader.lexiconInput = lexiconInput.clone();
		reader.memoryLexicon = memoryLexicon;
		reader.fileLimit = fileLimit;
		reader.queryAnalyzerPool = queryAnalyzerPool;
		reader.indexSetting = indexSetting;
		reader.indexFieldOption = indexFieldOption;
		reader.segmentDocumentCount = segmentDocumentCount;
		
		return reader;
	}
	
	public PostingDocs getPosting(CharVector singleTerm) throws IOException {
		if (memoryLexicon.size() == 0){
			return null;
		}
		
		if (singleTerm.length() == 0){
			return null;
		}

		long[] posInfo = new long[2];
		boolean found = memoryLexicon.binsearch(singleTerm, posInfo);

		long pos = -1;
		// cannot find in memory index, let's find it in file index
		// long tt = System.currentTimeMillis();
		if (found) {
			pos = posInfo[1];
		} else {
			lexiconInput.seek(posInfo[0]);
			while (lexiconInput.position() < fileLimit) {
				char[] term2 = lexiconInput.readUString();

				int cmp = compareKey(term2, singleTerm);

				if (cmp == 0) {
					pos = lexiconInput.readLong();
					if (logger.isDebugEnabled()) {
						logger.debug("search success = {} at field-{}", singleTerm, indexId);
					}
					break;
				} else if (cmp > 0) {
					// if term value is greater than this term, there's no such
					// word.
					// search fail
					if (logger.isDebugEnabled()) {
						logger.debug("search fail = {} at field[{}]", singleTerm, indexId);
					}
					break;
				} else {
					// skip reading pos
					lexiconInput.seek(lexiconInput.position() + IOUtil.SIZE_OF_LONG);
				}
			}
		}

		if (pos >= 0) {

			return getTermDocs(singleTerm, pos);
		}

		return null;
	}

	private PostingDocs getTermDocs(CharVector singleTerm, long pos) throws IOException {
		postingInput.seek(pos);
		int len = postingInput.readVInt();
		int count = postingInput.readInt();
		int lastDocNo = postingInput.readInt();

		PostingDoc[] termDocList = new PostingDoc[count];
		logger.debug(">>>>> create PostingDoc array size {} = {} / {}MB", singleTerm, count, Runtime.getRuntime().totalMemory() / (1024 * 1024));

		int prevId = -1;
		int docId = -1;
		for (int i = 0; i < count; i++) {
			if (prevId >= 0) {
				docId = postingInput.readVInt() + prevId + 1;
			} else {
				docId = postingInput.readVInt();
			}
			int tf = postingInput.readVInt();
			int[] positions = null;
			if (tf > 0 && indexFieldOption.isStorePosition()) {
				int prevPosition = -1;
				positions = new int[tf];
				for (int j = 0; j < tf; j++) {
					if (prevPosition >= 0) {
						positions[j] = postingInput.readVInt() + prevPosition + 1;
					} else {
						positions[j] = postingInput.readVInt();
					}
					prevPosition = positions[j];

				}

			}
//			logger.debug("getTermDocs {} >> {} : {} / {}", singleTerm, docId, tf, positions);

			termDocList[i] = new PostingDoc(docId, tf, positions);
			prevId = docId;

		}
		return new PostingDocs(singleTerm, termDocList, count);
	}

	protected PostingDocs getExtendedPosting(int indexFieldSequence, CharVector singleTerm) throws IOException {

		// SUFFIX SEARCH
		if (singleTerm.array().length > 0 && singleTerm.array()[0] == '*') {
			// 2012-02-09 swsong
			// 여러필드에 걸쳐서 prefix검색을 할때 두번째 필드부터는 singleTerm의 length가 변경된 상태여서 문제가
			// 생김.
			// 새로운 charVector를 만들어서 처리하는 것을 수정.
			CharVector cloneVector = singleTerm.clone();
			cloneVector.setStart(cloneVector.start() + 1);
			cloneVector.setLength(cloneVector.length() - 1);
			return getSuffixPosting(cloneVector);
			// PREFIX SEARCH
		} else if (singleTerm.charAt(singleTerm.length() - 1) == '*') {
			// remove last one
			CharVector cloneVector = singleTerm.clone();
			cloneVector.setLength(cloneVector.length() - 1);
			return getPrefixPosting(cloneVector);
		} else {
			// RANGE SEARCH
			int pos = 0;
			for (int i = singleTerm.start(); i < singleTerm.length(); i++, pos++) {
				char ch = singleTerm.array()[i];
				if (ch == '~') {
					CharVector startTerm = (CharVector) singleTerm.clone();
					startTerm.setLength(pos);
					CharVector endTerm = (CharVector) singleTerm.clone();
					endTerm.setLength(endTerm.length() - (pos + 1));
					endTerm.setStart(i + 1);
					// logger.debug("startCV = "+new String(startCV.array,
					// startCV.start, startCV.length));
					// logger.debug("endCV = "+new String(endCV.array,
					// endCV.start, endCV.length));

					// return getPosting(indexFieldSequence, startTerm);
					return getRangePosting(startTerm, endTerm);
				}
			}

		}
		// logger.debug("IGNORE Extended Search!");
		// Treat as a whole-word non-extended search
		return getPosting(singleTerm);
	}

	protected PostingDocs getPrefixPosting(CharVector singleTerm) throws IOException {
		if (memoryLexicon.size() == 0)
			return null;

		long[] posInfo = new long[2];
		boolean found = memoryLexicon.binsearch(singleTerm, posInfo);

		int foundCount = 0;
		long startPos = -1;

		long lexiconPos = posInfo[0];
		// logger.debug("lexiconPos = {}", lexiconPos);
		lexiconInput.seek(lexiconPos);

		while (lexiconInput.position() < fileLimit) {
			// lexiconInput
			char[] term2 = lexiconInput.readUString();
			int cmp = comparePrefixKey(term2, singleTerm);
			// logger.debug("compare key "+new String(term2)+" = "+cmp);

			// 작거나 같으면 prefix에 부합한다.2013-05-28 swsong.
			if (cmp <= 0) {
				long pos = lexiconInput.readLong();
				if (foundCount == 0) {
					startPos = pos;
				}
				foundCount++;
				// logger.debug("prefix term = {}", new String(term2));

			} else if (cmp > 0) {
				// if term value is greater than this term, there's no suchword.
				// search fail
				// logger.debug("search finish! = "+new String(singleTerm.array,
				// singleTerm.start, singleTerm.length));
				break;
			}
		}

		// logger.debug("startPos = {}, foundCount = {}", startPos, foundCount);
		return makeTermDocs(singleTerm, startPos, foundCount);
	}

	protected PostingDocs getSuffixPosting(CharVector singleTerm) throws IOException {
		if (memoryLexicon.size() == 0)
			return null;

		long[] posInfo = new long[2];
		boolean found = memoryLexicon.binsearch(singleTerm, posInfo);

		int foundCount = 0;
		long startPos = -1;

		long lexiconPos = posInfo[0];
		// logger.debug("lexiconPos = {}", lexiconPos);
		lexiconInput.seek(lexiconPos);

		boolean isIncludSearch = false;

		if (singleTerm.charAt(singleTerm.length() - 1) == '*') {
			singleTerm.setLength(singleTerm.length() - 1);
			isIncludSearch = true;
		}

		while (lexiconInput.position() < fileLimit) {
			// lexiconInput
			char[] term2 = lexiconInput.readUString();
			int cmp = isIncludSearch ? compareIncludingKey(term2, singleTerm) : compareSuffixKey(term2, singleTerm);
			// logger.debug("compare key "+new String(term2)+" = "+cmp);
			if (cmp == 0) {
				long pos = lexiconInput.readLong();
				if (foundCount == 0)
					startPos = pos;
				foundCount++;
				// logger.debug("prefix term = {}", new String(term2));

			} else if (cmp > 0) {
				// if term value is greater than this term, there's no such
				// word.
				// search fail
				break;
			} else {
				// skip reading pos
				lexiconInput.seek(lexiconInput.position() + IOUtil.SIZE_OF_LONG);
				// retry!
			}
		}

		logger.debug("startPos = {}, foundCount = {}", startPos, foundCount);
		return makeTermDocs(singleTerm, startPos, foundCount);
	}

	protected PostingDocs getRangePosting(CharVector startTerm, CharVector endTerm) throws IOException {
		if (memoryLexicon.size() == 0)
			return null;
		logger.debug("Range : {} ~ {}", startTerm, endTerm);

		int cmpValid = 0;
		char[] startTermChars = new char[startTerm.length()];
		System.arraycopy(startTerm.array(), startTerm.start(), startTermChars, 0, startTerm.length());
		cmpValid = compareKey(startTermChars, endTerm);
		// ensure startTerm <= endTerm
		if (cmpValid > 0) {
			return new PostingDocs(startTerm, 0);
		}

		/*
		 * 1. find startTerm
		 */
		long[] posInfo = new long[2];
		boolean found = memoryLexicon.binsearch(startTerm, posInfo);

		// if(isNumericField)
		// found = memoryLexicon.binsearchNumeric(startTerm,
		// posInfo);
		// else
		// found = memoryLexicon.binsearch(startTerm, posInfo);

		int foundCount = 0;
		long startPos = -1;

		long lexiconPos = posInfo[0];
		logger.debug("lexiconPos = {} / limit = {}", lexiconPos, fileLimit);
		lexiconInput.seek(lexiconPos);

		/*
		 * startTerm
		 */
		char[] term2 = null;
		int cmp = 0;

		// char[] prevTerm = null;
		// long prevPos = -1;
		boolean isStarted = false;

		while (lexiconInput.position() < fileLimit) {
			// lexiconInput
			term2 = lexiconInput.readUString();
			startPos = lexiconInput.readLong();
			// logger.debug("Test term = "+new String(term2));
			cmp = compareKey(term2, startTerm);

			if (cmp == 0) {
				// logger.debug("range start1 term = "+new String(term2));
				isStarted = true;
				// isMatched = true;
				foundCount++;
				break;
			} else if (cmp > 0) {
				// use prev term, because prev term is closest among the
				// smallers than this term.
				// term2 = prevTerm;
				// startPos = prevPos;
				// logger.debug("range start2 term = {}", new String(term2));
				isStarted = true;

				if (compareKey(term2, endTerm) == 0) {
					// 종료텀이 처음에 발견되었을때.
					foundCount++;
				}
				break;
			}
		}

		if (!isStarted)
			return null;

		// if(isMatched && startTerm.equals(endTerm)){
		// //시작과 끝이 같고, 일치하는 텀이 있다면. 하나발견된것임.
		// foundCount = 1;
		// }
		/*
		 * endTerm
		 */
		// logger.debug("while {} < {}",lexiconInput.position(),
		// fileLimit );
		while (lexiconInput.position() < fileLimit) {

			cmp = compareKey(term2, endTerm);
			// logger.debug("compareKey == {}, {}:{}", new Object[]{cmp, term2,
			// endTerm});
			if (cmp == 0) {
				logger.debug("range end term(inclusive) = {}", new String(term2));
				foundCount++;
				break;
			} else if (cmp > 0) {
				break;
			} else {
				// logger.debug("range term = "+new String(term2));
				foundCount++;
				// continue
			}
			// logger.debug("lexiconInput.position()="+lexiconInput.position());
			term2 = lexiconInput.readUString();
			lexiconInput.readLong();
			// lexiconInput.position(lexiconInput.position() +
			// IOUtil.SIZEOFLONG);
			// logger.debug("Next term = "+new String(term2)+", "+term2.length);
		}

		// logger.debug("startPos = "+startPos+", foundCount = "+foundCount);
		return makeTermDocs(startTerm, startPos, foundCount);

	}

	// thread-unsafe! 호출하는 메서드에서 thread-safe하게 호출해야한다.
	int mpseq = 0;

	private PostingDocs makeTermDocs(CharVector term, long startPos, int foundCount) throws IOException {

		if (foundCount > 0) {
			FixedMinHeap<PostingDocsReader> heap = new FixedMinHeap<PostingDocsReader>(foundCount);
			mpseq++;

			long pos = startPos;
			postingInput.seek(pos);

			List<PostingDocs> termDocsList = new ArrayList<PostingDocs>(foundCount);

			for (int c = 0; c < foundCount; c++) {
				int prevId = -1;
				// 위치정보를 가지고 포스팅을 읽는다.
				int len = postingInput.readVInt();
				int count = postingInput.readInt();
				int lastDocNo = postingInput.readInt();

				// logger.debug("prefix posting {} / {}", c, foundCount);
				// logger.debug("prefix posting len = {}", len);
				// logger.debug("prefix posting count = {}", count);
				// logger.debug("prefix posting lastDocNo = {}", lastDocNo);

				PostingDoc[] termDocList = new PostingDoc[count];

				int docId = -1;

				for (int i = 0; i < count; i++) {
					if (prevId >= 0) {
						docId = postingInput.readVInt() + prevId + 1;
					} else {
						docId = postingInput.readVInt();
					}

					int tf = postingInput.readVInt();

					int[] positions = null;
					if (indexFieldOption.isStorePosition()) {
						int prevPosition = -1;
						positions = new int[tf];
						for (int j = 0; j < tf; j++) {
							if (prevPosition >= 0) {
								positions[j] = postingInput.readVInt() + prevPosition + 1;
							} else {
								positions[j] = postingInput.readVInt();
							}
							prevPosition = positions[j];
						}

					}

					termDocList[i] = new PostingDoc(docId, tf, positions);

					prevId = docId;

				}

				// TermDocsReader r = new TermDocsReader(new TermDocs(indexFieldSequence, term, termDocList, count));

				termDocsList.add(new PostingDocs(term, termDocList, count));
				// if (r.next()) {
				// heap.push(r);
				// }

			}// for

			return new PostingDocsMerger(termDocsList).merge(term, 1024);
		}
		return null;
	}


	private int compareKey(char[] t, CharVector term) {

		int len1 = t.length;
		int len2 = term.length();

		int len = len1 < len2 ? len1 : len2;

		for (int i = 0; i < len; i++) {
			char ch = term.charAt(i);

			if (t[i] != ch) {
				return t[i] - ch;
			}
		}

		return len1 - len2;
	}

	private int comparePrefixKey(char[] t, CharVector term) {
		int len1 = t.length;
		int len2 = term.length();

		int len = len1 < len2 ? len1 : len2;

		for (int i = 0; i < len; i++) {
			char ch = term.charAt(i);

			if (t[i] != ch) {
				return t[i] - ch;
			}
		}

		// if come here, they're partially matched.
		if (len1 >= len2) {
			// term is prefix
			return 0;
		}
		// char[] t is prefix. we don't want this case.
		return len1 - len2;
	}

	private int compareSuffixKey(char[] t, CharVector term) {

		// enable to include term
		if (t.length >= term.length()) {
			for (int i = 0, j = 0; i < t.length; i++) {
				if ((t.length - i) < j) {
					break;
				}
				if (t[t.length - 1 - i] == term.charAt(term.length() - 1 - j)) {
					if (++j == term.length()) {
						return 0;
					}
				} else {
					j = 0;
					continue;
				}
			}
		}
		return -1;
	}

	private int compareIncludingKey(char[] t, CharVector term) {

		// enable to include term
		if (t.length >= term.length()) {
			for (int i = 0, j = 0; i < t.length; i++) {
				if ((t.length - i) < j) {
					break;
				}
				if (t[i] == term.charAt(j)) {
					if (++j == term.length()) {
						return 0;
					}
				} else {
					j = 0;
					continue;
				}
			}
		}
		return -1;
	}

	public Analyzer getQueryAnalyzerFromPool() {
		return queryAnalyzerPool.getFromPool();
	}

	public void releaseQueryAnalyzerToPool(Analyzer analyzer) {
		queryAnalyzerPool.releaseToPool(analyzer);
	}

	public IndexSetting indexSetting() {
		return indexSetting;
	}

	public IndexFieldOption indexFieldOption(){
		return indexFieldOption;
	}
	public SearchMethod createSearchMethod(AbstractSearchMethod searchMethod){
		//index input은 clone하여 각자 사용한다. 
		searchMethod.init(this.memoryLexicon, this.lexiconInput.clone(), this.postingInput.clone(), this.indexFieldOption, this.segmentDocumentCount);
		return searchMethod;
	}
	
}
