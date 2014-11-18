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

package org.fastcatsearch.ir.index.temp;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.index.IndexFieldOption;
import org.fastcatsearch.ir.io.BufferedFileOutput;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.ir.io.CharVector;
import org.fastcatsearch.ir.io.IOUtil;
import org.fastcatsearch.ir.io.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempSearchFieldMerger {
	protected static Logger logger = LoggerFactory.getLogger(TempSearchFieldMerger.class);

	protected int[] heap;
	protected TempSearchFieldReader[] reader;
	protected String indexId;
	protected int flushCount;
	protected BytesDataOutput tempPostingOutput;
	
	private int bufferCount;
	private CharVector cv;
	private CharVector cvOld;
	protected int totalCount;
	protected int prevDocNo;
	private BytesRef[] buffers;

	public TempSearchFieldMerger(String indexId, List<Long> flushPosition, File tempFile) throws IOException {
		this.indexId = indexId;
		this.flushCount = flushPosition.size();
		reader = new TempSearchFieldReader[flushCount];
		for (int m = 0; m < flushCount; m++) {
			reader[m] = new TempSearchFieldReader(m, indexId, tempFile, flushPosition.get(m));
			reader[m].next();
		}

		tempPostingOutput = new BytesDataOutput(1024 * 1024);
		// 동일한 단어는 최대 flush갯수 만큼 buffer 배열에 쌓이게 된다.
		buffers = new BytesRef[flushCount];
	}

	public void mergeAndMakeIndex(File baseDir, int indexInterval, IndexFieldOption fieldIndexOption) throws IOException {
		logger.debug("**** mergeAndMakeIndex ****");
		logger.debug("flushCount={}", flushCount);

		if (flushCount <= 0) {
			return;
		}

		IndexOutput postingOutput = new BufferedFileOutput(baseDir, IndexFileNames.getSearchPostingFileName(indexId));
		IndexOutput lexiconOutput = new BufferedFileOutput(baseDir, IndexFileNames.getSearchLexiconFileName(indexId));
		IndexOutput indexOutput = new BufferedFileOutput(baseDir, IndexFileNames.getSearchIndexFileName(indexId));

		try {
			postingOutput.writeInt(fieldIndexOption.value());

			// to each field
			logger.debug("## MERGE field = {}", indexId);

			makeHeap(flushCount);

			int termCount = 0;
			int indexTermCount = 0;

			lexiconOutput.writeInt(termCount);// termCount
			indexOutput.writeInt(indexTermCount);// indexTermCount

			CharVector term = new CharVector();
			while (readNextTempIndex(term)) {
				int len = (int) tempPostingOutput.position();
				int count = totalCount;
				int lastDocNo = prevDocNo;
				int firstDocNo = IOUtil.readVInt(tempPostingOutput.array(), 0);
				int sz = IOUtil.lenVariableByte(firstDocNo);

				len -= sz;

				int sz2 = IOUtil.lenVariableByte(firstDocNo);

				long postingPosition = postingOutput.position();

				int len2 = IOUtil.SIZE_OF_INT * 2 + sz2 + len;
				if (len2 < 8)
					throw new IOException("Terrible Error!! " + len2);
				
				
				//1. Write Posting
				postingOutput.writeVInt(len2);
				postingOutput.writeInt(count);
				postingOutput.writeInt(lastDocNo);
				postingOutput.writeVInt(firstDocNo);
				postingOutput.writeBytes(tempPostingOutput.array(), sz, len);

				
				//2. Write Lexicon
				long lexiconPosition = lexiconOutput.position();
				lexiconOutput.writeUString(term.array(), term.start(), term.length());
				lexiconOutput.writeLong(postingPosition);
				
				//3. Write Index
				if (indexInterval > 0 && (termCount % indexInterval) == 0) {
					indexOutput.writeUString(term.array(), term.start(), term.length());
					indexOutput.writeLong(lexiconPosition);
					indexOutput.writeLong(postingPosition);
					indexTermCount++;
				}
				termCount++;
				
			}


			// Write term count on head position
			// long prevPos = lexiconOutput.position();
			// lexiconOutput.seek(prevPos);
			if (termCount > 0) {
				lexiconOutput.seek(0);
				lexiconOutput.writeInt(termCount);
				indexOutput.seek(0);
				indexOutput.writeInt(indexTermCount);
			} else {
				// 이미 indexTermCount는 0으로 셋팅되어 있으므로 기록할 필요없음.
			}
			logger.debug("## write index [{}] termCount[{}] indexTermCount[{}] indexInterval[{}]", indexId, termCount, indexTermCount, indexInterval);

			lexiconOutput.flush();
			indexOutput.flush();
			postingOutput.flush();

		} finally {
			IOException exception = null;

			try {
				if (postingOutput != null) {
					postingOutput.close();
				}
			} catch (IOException e) {
				exception = e;
			}
			try {
				if (lexiconOutput != null) {
					lexiconOutput.close();
				}
			} catch (IOException e) {
				exception = e;
			}
			try {
				if (indexOutput != null) {
					indexOutput.close();
				}
			} catch (IOException e) {
				exception = e;
			}

			if (exception != null) {
				throw exception;
			}
		}
	}

	// 여러번 flush된 임시 posing 파일에서 정렬된 단어들을 읽어들여 posting을 tempPostingOutput 하나로 머징한다.
	protected boolean readNextTempIndex(CharVector term) throws IOException {
		tempPostingOutput.reset();
		boolean termMade = false;

		// int kk = 0;
		while (true) {
			int idx = heap[1];
			cv = reader[idx].term();
			if (cv == null && cvOld == null) {
				// if cv and cvOld are null, it's done
				return false;
			}

			// cv == null일경우는 모든 reader가 종료되어 null이 된경우이며
			// cvOld 와 cv 가 다른 경우는 머징시 텀이 바뀐경우. cvOld를 기록해야한다.
			if ((cv == null || !cv.equals(cvOld)) && cvOld != null) {
				// merge buffers
				prevDocNo = -1;
				totalCount = 0;
				for (int k = 0; k < bufferCount; k++) {
					BytesRef buf = buffers[k];
					// buf.reset();

					// count 와 lastNo를 읽어둔다.
					int count = IOUtil.readInt(buf);
					int lastDocNo = IOUtil.readInt(buf);
					totalCount += count;
					// logger.debug("count="+count);
					if (k == 0) {
						// 첫번째 문서번호부터 끝까지 기록한다.
						tempPostingOutput.writeBytes(buf.array(), buf.pos(), buf.remaining());
					} else {
						int firstNo = IOUtil.readVInt(buf);
						int newDocNo = firstNo - prevDocNo - 1;
//						logger.debug("newDocNo={}, firstNo={}, prevDocNo={}", newDocNo, firstNo, prevDocNo);

						IOUtil.writeVInt(tempPostingOutput, newDocNo);
						tempPostingOutput.writeBytes(buf.array(), buf.pos(), buf.remaining());
					}
					prevDocNo = lastDocNo;
				}

				termMade = true;
				term.init(cvOld.array(), cvOld.start(), cvOld.length());

				bufferCount = 0;

			}

			if(bufferCount < buffers.length){
				try {
					buffers[bufferCount++] = reader[idx].buffer();
				} catch (ArrayIndexOutOfBoundsException e) {
					logger.info("### bufferCount= {}, buffers.len={}, idx={}, reader={}", bufferCount, buffers.length, idx, reader.length);
					logger.error("dup terms", e);
				}
			}else{
				logger.warn("wrong! {}", cv);
				logger.info("### bufferCount= {}, buffers.len={}, idx={}, reader={}", bufferCount, buffers.length, idx, reader.length);
			}
			// backup cv to old
			cvOld = cv;

			reader[idx].next();

			heapify(1, flushCount);

			if (termMade) {
				return true;
			}
		} // while(true)

	}

	public void close() throws IOException {
		IOException exception = null;
		for (int i = 0; i < flushCount; i++) {
			if (reader[i] != null) {
				try {
					reader[i].close();
				} catch (IOException e) {
					exception = e;
				}
			}
		}
		if (exception != null) {
			throw exception;
		}
	}

	protected void makeHeap(int heapSize) {
		heap = new int[heapSize + 1];
		// index starts from 1
		for (int i = 0; i < heapSize; i++) {
			heap[i + 1] = i;
		}

		int n = heapSize >> 1; // last inner node index

		for (int i = n; i > 0; i--) {
			heapify(i, heapSize);
		}

	}

	protected void heapify(int idx, int heapSize) {

		int temp = -1;
		int child = -1;

		while (idx <= heapSize) {
			int left = idx << 1;// *=2
			int right = left + 1;

			if (left <= heapSize) {
				if (right <= heapSize) {
					// 키워드가 동일할 경우 먼저 flush된 reader가 우선해야, docNo가 오름차순 정렬순서대로 올바로 기록됨.
					// flush후 머징시 문제가 생기는 버그 해결됨 2013-5-21 swsong
					int c = compareKey(left, right);
					if (c < 0) {
						child = left;
					} else if (c > 0) {
						child = right;
					} else {
						// 하위 value 둘이 같아서 seq확인.
						// 같다면 id가 작은게 우선.
						int a = heap[left];
						int b = heap[right];
						if (reader[a].sequence() < reader[b].sequence()) {
							child = left;
						} else {
							child = right;
						}
					}
				} else {
					// if there is no right el.
					child = left;
				}
			} else {
				// no children
				break;
			}

			// compare and swap
			int c = compareKey(child, idx);
			if (c < 0) {
				temp = heap[child];
				heap[child] = heap[idx];
				heap[idx] = temp;
				idx = child;
				// System.out.println("idx1="+idx);
			} else if (c == 0) {
				// 하위와 자신의 value가 같아서 seq확인
				// 같다면 seq가 작은게 우선.
				int a = heap[idx];
				int b = heap[child];
				if (reader[a].sequence() > reader[b].sequence()) {
					// 하위의 seq가 작아서 child채택!
					temp = heap[child];
					heap[child] = heap[idx];
					heap[idx] = temp;
					idx = child;
				} else {
					// 내것을 그대로 사용.
					// sorted
					break;
				}
			} else {
				// sorted, then do not check child
				break;
			}

		}
	}

	protected int compareKey(int one, int another) {

		int a = heap[one];
		int b = heap[another];

		return compareKey(reader[a].term(), reader[b].term());
	}

	protected int compareKey(CharVector term1, CharVector term2) {

		// reader gets EOS, returns null
		if (term1 == null && term2 == null) {
			return 0;
		} else if (term1 == null)
			return 1;
		else if (term2 == null)
			return -1;

		int len = (term1.length() < term2.length()) ? term1.length() : term2.length();

		for (int i = 0; i < len; i++) {
			if (term1.charAt(i) != term2.charAt(i))
				return term1.charAt(i) - term2.charAt(i);
		}

		return term1.length() - term2.length();
	}
}
