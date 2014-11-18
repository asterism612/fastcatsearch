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

import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.io.BufferedFileInput;
import org.fastcatsearch.ir.io.BytesBuffer;
import org.fastcatsearch.ir.io.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryKeyIndexReader implements BytesToIntReader, Cloneable {
	private static Logger logger = LoggerFactory.getLogger(PrimaryKeyIndexReader.class);

	private IndexInput input;
	private byte[][] keys;
	private long[] pos;
	private int count; // key 갯수
	private long limit;

	private PrimaryKeyIndexReader() {
	}

	public PrimaryKeyIndexReader(File pkFile, File pkIndexFile) throws IOException {

		input = new BufferedFileInput(pkFile);
		IndexInput indexInput = new BufferedFileInput(pkIndexFile);

		init(input, indexInput);
	}

	public PrimaryKeyIndexReader(File dir, String filename) throws IOException {
		String pkIndexFilename = IndexFileNames.getIndexFileName(filename);

		input = new BufferedFileInput(dir, filename);
		IndexInput indexInput = new BufferedFileInput(dir, pkIndexFilename);

		init(input, indexInput);
	}

	public PrimaryKeyIndexReader(IndexInput input, IndexInput indexInput) throws IOException {
		init(input, indexInput);
	}

	public void init(IndexInput input, IndexInput indexInput) throws IOException {
		this.input = input;

		limit = input.length();

		this.count = input.readInt();
		logger.debug("** count={}", count);
		try {
			int idxCount = indexInput.readInt();
			// logger.debug("** idxCount={}", idxCount);
			keys = new byte[idxCount][];
			pos = new long[idxCount];

			for (int i = 0; i < idxCount; i++) {
				int len = indexInput.readVInt();
				keys[i] = new byte[len];
				indexInput.readBytes(keys[i], 0, len);
				pos[i] = indexInput.readLong();
			}
		} finally {
			if (indexInput != null) {
				indexInput.close();
			}
		}
	}

	public PrimaryKeyIndexReader clone() {
		PrimaryKeyIndexReader reader = new PrimaryKeyIndexReader();
		reader.input = input.clone();
		reader.keys = keys;
		reader.pos = pos;
		reader.count = count;
		reader.limit = limit;
		return reader;

	}

	public int count() {
		return count;
	}

	public void close() throws IOException {
		input.close();
	}

	public int get(byte[] data) throws IOException {
		return get(data, 0, data.length);
	}

	@Override
	public int get(BytesBuffer bytesBuffer) throws IOException {
		return get(bytesBuffer.bytes, bytesBuffer.offset, bytesBuffer.length);
	}

	@Override
	public int get(byte[] data, int offset, int length) throws IOException {
		// if index is empty, it has no entry.
		if (keys.length == 0)
			return -1;

		int idx = binsearch(data, offset, length);
		byte[] test = new byte[length];
		// long position = pos[idx] + dataBasePosition;
		long position = pos[idx];
		// logger.debug("input ="+input.size()+", position="+position+", length="+length + ", "+pos[idx]+", "+dataBasePosition);
		// try{
		// lock.lock();
		input.seek(position);

		while (input.position() < limit) {
			int len = input.readVInt();
			// if(length == len){
			if (length < len) {
				test = new byte[len];
			}
			input.readBytes(test, 0, len);
			int docNo = input.readInt();
			int ret = compare(test, len, data, offset, length);
//			logger.debug("compare = "+new String(test, 0, len)+" : "+new String(data, offset, length)+" docNo="+docNo+", ret="+ret);
			if (ret == 0)
				return docNo;
			else if (ret > 0)
				return -1;
			// }
		}
		// }finally{
		// lock.unlock();
		// }
		// get EOF
		return -1;
	}

	// find closest smaller one's position
	private int binsearch(byte[] data, int offset, int length) {
		int left = 0;
		int right = keys.length - 1;
		int mid = -1;

		while (left <= right) {
			mid = (int) ((left + right) / 2);
			int ret = compare(keys[mid], keys[mid].length, data, offset, length);
			if (ret == 0) {
				return mid;
			}

			else if (ret < 0)
				left = mid + 1;
			else
				right = mid - 1;
		}

		mid = right < mid ? right : mid;
		if (mid < 0)
			mid = 0;

		if (compare(keys[mid], keys[mid].length, data, offset, length) > 0 && mid > 0) {
			mid--;
		}

		return mid;

	}

	private int compare(byte[] key, int keyLen, byte[] data, int offset, int length) {
		int len = keyLen < length ? keyLen : length;
		
		//둘중하나 길이가 0이면 비교없이 길이로 승부한다.
		if(len == 0) {
			return keyLen - length;
		}
		// logger.debug("keyLen={}, length={}", keyLen, length);
		for (int i = 0; i < len; i++) {
			if (key[i] != data[offset + i]) {
				// logger.debug("cmp >> {}:{}", key[i], data[offset + i]);
				return key[i] - data[offset + i];
			}
		}

		if (length > keyLen) {
			int remain = length - keyLen;
			for (int i = 0; i < remain; i++) {
				if (data[offset + len + i] != 0) {
					// 남은 데이터가 0이 아니면 다른 데이터이다.
					return keyLen - length;
				} else {
					// 남은 데이터가 모두 0이라면 무시해도 좋다.
				}
			}
		}
		// 여기까지오면 같은 데이터라고 볼수 있다.
		return 0;
	}

}
