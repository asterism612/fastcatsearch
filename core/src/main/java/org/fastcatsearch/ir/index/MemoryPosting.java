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

import java.io.IOException;
import java.util.Arrays;

import org.fastcatsearch.al.HashFunctions;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.io.BytesBuffer;
import org.fastcatsearch.ir.io.CharVector;
import org.fastcatsearch.ir.io.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class MemoryPosting {
	protected static Logger logger = LoggerFactory.getLogger(MemoryPosting.class);
	protected static final HashFunctions hfunc = HashFunctions.RSHash;

	protected int[] bucket;
	protected char[] keyArray;
	protected int[] keyPos;
	protected int[] nextIdx;
	protected PostingBuffer[] postingArray;

	protected int bucketSize;
	protected int length;
	protected int count;
	protected int keyArrayLength;
	protected int keyUseLength;
	protected boolean isIgnoreCase;

	public MemoryPosting(int size) {
		this(size, false);
	}

	public MemoryPosting(int size, boolean isIgnoreCase) {
		bucketSize = size;
		length = bucketSize;
		count = 0;
		keyArrayLength = bucketSize * 5;
		keyUseLength = 0;
		this.isIgnoreCase = isIgnoreCase;
		bucket = new int[bucketSize];
		keyArray = new char[keyArrayLength];
		keyPos = new int[length];
		nextIdx = new int[length];
		postingArray = new PostingBuffer[length];

		Arrays.fill(bucket, -1);
	}

	protected PostingBuffer newPostingBuffer() {
		return new PostingBuffer();
	}

	public long save(IndexOutput output) throws IOException {
		// 하나의 파일에 블럭단위로 write한다. 맨앞에 데이터 길이필요

		logger.debug("MemoryPosting term-count = {}", count);
		// sort
		int[] sortedID = new int[count];
		for (int i = 0; i < count; i++) {
			sortedID[i] = i;
		}

		if (count > 0) {
			long st = System.currentTimeMillis();
			logger.debug("MemoryPosting term sort...");
			quickSort(sortedID, 0, count - 1);
			logger.debug("Sort Done. time = {}ms", System.currentTimeMillis() - st);
		}
		// 기록위치
		long outPos = output.position();

		// 텀갯수
		output.writeInt(count);
		// logger.debug("term count = {}", count);

		for (int i = 0; i < count; i++) {
			int id = sortedID[i];
			int pos = keyPos[id];
			int len = -1;
			// 마지막 원소이면
			if (id == count - 1) {
				len = keyUseLength - pos;
			} else {
				len = keyPos[id + 1] - pos;
			}

			output.writeUString(keyArray, pos, len);
			// logger.debug("key>> {}", new String(keyArray, pos, len));
			if (postingArray[id] == null) {
				logger.error("id={}, len={}, term={}", id, len, new String(keyArray, pos, len));
			}
			postingArray[id].finish();
			BytesBuffer buf = postingArray[id].buffer();
			// 데이터길이
			output.writeVInt(buf.length());
			// logger.debug("term = {} >> {}", new String(keyArray, pos, len), buf.length());
			if (buf.length() > 0) {
				output.writeBytes(buf);
			} else {
				logger.error("buffer empty >> {} = {} ,data=0 ", id, new String(keyArray, pos, len));
				// 버퍼가 비어있도록 진행하도록 수정.
				// throw new IOException("buf is empty");
			}
		}

		logger.debug("==================");
		logger.debug("outPos = {}, count = {}, end = {}", outPos, count, output.position());

		return outPos;
	}

	private void quickSort(int[] ids, int first, int last) {
		if (last <= 0)
			return;

		int stackMaxSize = (int) ((Math.log(last - first + 1) + 3) * 2);
		int[][] stack = new int[stackMaxSize][2];

		int pivotId = 0, sp = 0;
		int left = 0, right = 0;

		while (true) {
			while (first < last) {
				left = first;
				right = last;
				int median = (left + right) / 2;

				// move pivot to left most.
				int tmp = ids[left];
				ids[left] = ids[median];
				ids[median] = tmp;
				pivotId = ids[left];

				while (left < right) {
					while (compareKey(ids[right], pivotId) >= 0 && (left < right))
						right--;

					if (left != right) {
						ids[left] = ids[right];
						left++;
					}

					while (compareKey(ids[left], pivotId) <= 0 && (left < right))
						left++;

					if (left != right) {
						ids[right] = ids[left];
						right--;
					}
				}

				ids[left] = pivotId;

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

	private int compareKey(int id, int id2) {
		int pos = keyPos[id];
		int len = -1;

		if (id == count - 1)
			len = keyUseLength - pos;
		else
			len = keyPos[id + 1] - pos;

		int pos2 = keyPos[id2];
		int len2 = -1;

		if (id2 == count - 1)
			len2 = keyUseLength - pos2;
		else
			len2 = keyPos[id2 + 1] - pos2;

		int length = (len < len2) ? len : len2;

		for (int i = 0; i < length; i++) {
			if (keyArray[pos + i] != keyArray[pos2 + i])
				return keyArray[pos + i] - keyArray[pos2 + i];
		}

		return len - len2;
	}

	public void add(CharVector term, int docNo) throws IRException {
		add(term, docNo, 0);
	}

	public void add(CharVector term, int docNo, int position) throws IRException {
		if (term == null || term.length() == 0) {
			return;
		}
		PostingBuffer p = get(term);

		if (p == null) {
			p = newPostingBuffer();
			put0(term, p);
		}
		// logger.debug("term >> {}", term);
		p.addOne(docNo, position);
	}

	private boolean isTheSame(CharVector term, int id) {
		int pos = keyPos[id];
		int len = -1;
		// last el?
		if (id == count - 1)
			len = keyUseLength - pos;
		else
			len = keyPos[id + 1] - pos;

		// logger.debug(term+" , term.length="+term.length+", len="+len);
		if (term.length() == len) {
			if (isIgnoreCase) {
				for (int i = 0; i < len; i++) {
					if (toUpperChar(term.charAt(i)) != keyArray[pos + i]){
						return false;
					}
				}
			}else{
				for (int i = 0; i < len; i++) {
					if (term.charAt(i) != keyArray[pos + i]){
						return false;
					}
				}
			}
			return true;
		}
		return false;
	}

	private char toUpperChar(int ch) {
		if ((ch <= 'z' && ch >= 'a')) { // 소문자이면..
			ch -= 32;
		}
		return (char) ch;
	}

	private PostingBuffer put0(CharVector term, PostingBuffer p) {
		int hashValue = hfunc.hash(term, bucketSize, isIgnoreCase);

		int prev = -1;
		int idx = bucket[hashValue];

		while (idx >= 0) {
			if (isTheSame(term, idx))
				break;

			prev = idx;
			idx = nextIdx[idx];
		}

		if (idx >= 0) {
			// duplicated term
			if (prev != -1) {
				// put a link to the front
				nextIdx[prev] = nextIdx[idx];
				nextIdx[idx] = bucket[hashValue];
				bucket[hashValue] = idx;
			}// else let it be
		} else {
			// new term
			idx = getNextIdx();

			int needSize = keyUseLength + term.length();
			if(needSize > keyArrayLength) {
				while(needSize > keyArrayLength) {
					keyArrayLength *= 1.2;
				}
				char[] newArray = new char[keyArrayLength];
				System.arraycopy(keyArray, 0, newArray, 0, keyUseLength);
				keyArray = newArray;
			}
//			if (keyUseLength + term.length() >= keyArrayLength) {
//				keyArrayLength *= 1.2;
//				char[] newArray = new char[keyArrayLength];
//				System.arraycopy(keyArray, 0, newArray, 0, keyUseLength);
//				keyArray = newArray;
//			}
			keyPos[idx] = keyUseLength;

			if (isIgnoreCase) {
				for (int i = 0; i < term.length(); i++) {
					keyArray[keyUseLength++] = toUpperChar(term.charAt(i));
				}
			} else {
				for (int i = 0; i < term.length(); i++) {
					keyArray[keyUseLength++] = term.charAt(i);
				}
			}

			nextIdx[idx] = -1;
			if (prev != -1)
				nextIdx[prev] = idx;
			else
				bucket[hashValue] = idx;

		}

		PostingBuffer old = postingArray[idx];
		postingArray[idx] = p;

		return old;
	}

	public PostingBuffer get(CharVector term) {
		int hashValue = hfunc.hash(term, bucketSize, isIgnoreCase);
		int idx = bucket[hashValue];

		// logger.debug(term+" = "+hashValue+", idx="+idx);
		while (idx >= 0) {
			if (isTheSame(term, idx)) {
				break;
			}

			idx = nextIdx[idx];
		}

		if (idx < 0)
			return null; // 검색실패
		else {
			return postingArray[idx];
		}
	}

	private int getNextIdx() {
		if (count >= length) {
			int newLength = (int) (length * 1.2);
			// logger.debug("Grow length = "+length+" => "+newLength+", new int * 2, new PostingBuffer[], arraycopy * 3");
			int[] newKeyPos = new int[newLength];
			int[] newNext = new int[newLength];
			PostingBuffer[] newTermPosting = new PostingBuffer[newLength];

			System.arraycopy(keyPos, 0, newKeyPos, 0, count);
			System.arraycopy(nextIdx, 0, newNext, 0, count);
			System.arraycopy(postingArray, 0, newTermPosting, 0, count);

			keyPos = newKeyPos;
			nextIdx = newNext;
			postingArray = newTermPosting;
			length = newLength;
		}
		return count++;
	}

	public int workingMemorySize() {
		int size = 0;
		for (int i = 0; i < postingArray.length; i++)
			if (postingArray[i] != null)
				size += (postingArray[i].size() + 80);

		size += keyUseLength * 2;
		size += bucket.length * 4;
		size += count * 8; // keyPos(4), nextIdx(4)
		return size;
	}

	public int staticMemorySize() {
		int size = 0;
		for (int i = 0; i < postingArray.length; i++)
			if (postingArray[i] != null)
				size += (postingArray[i].size() + 80);

		size += keyArrayLength * 2;
		size += bucket.length * 4;
		size += keyPos.length * 4;
		size += nextIdx.length * 4;

		return size;
	}

	public void clear() {
		Arrays.fill(bucket, -1);
		Arrays.fill(nextIdx, -1);
		// posting array는 지워준다.
		for (int i = 0; i < postingArray.length; i++) {
			postingArray[i] = null;
		}
		count = 0;
		keyUseLength = 0;
		// keyarray배열은 그대로 재사용한다.

	}

	// entry count
	public int count() {
		return count;
	}

}
