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

package org.fastcatsearch.ir.util;

import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.index.IndexFieldOption;
import org.fastcatsearch.ir.io.BufferedFileInput;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * .lexicon 파일에서 텀과 색인포지션정보를 stdout으로 봅아낸다.
 * 사용법은 java LexiconChecker <검색필드명> <세그먼트경로> <리비전번호> <찾고자하는도큐먼트번호>
 */
public class LexiconPostingChecker {
	
	private String id;
	private File dir;
	BufferedFileInput indexInput;
	private int findDocNo;
	
	public static void main(String[] args) throws IOException {
		String id = args[0];
		File dir = new File(args[1]);
		
		int docNo = -1;
		
		if(args.length > 2) {
			docNo = Integer.parseInt(args[2]);
		}
		
		LexiconPostingChecker checker = new LexiconPostingChecker(id, dir, docNo);
		checker.list(System.out);
		checker.close();
	}
	
	
	public LexiconPostingChecker(String id, File dir, int findDocNo) throws IOException{
		this.id = id;
		this.dir = dir;
		this.findDocNo = findDocNo;
		
		System.out.println("Check dir = "+dir.getAbsolutePath());

		indexInput = new BufferedFileInput(dir, IndexFileNames.getSearchLexiconFileName(id));
	}
	
	public void close() throws IOException{
		indexInput.close();
	}

	public void list(PrintStream output) throws IOException{
		int indexSize = indexInput.readInt();
		output.println("Memory indexsize = "+indexSize);
		
		StringBuilder findWord = new StringBuilder();
		
		for (int k = 0; k < indexSize; k++) {
			String string = new String(indexInput.readUString());
			long inputOffset = indexInput.readLong();
			
			//output.println("word="+string+" ,"+ inputOffset);
			BufferedFileInput postingInput = new BufferedFileInput(dir , IndexFileNames.getSearchPostingFileName(id));
			
			BufferedFileInput clone = postingInput.clone();
			
			IndexFieldOption indexFieldOption = new IndexFieldOption(clone.readInt());
			
			//output.println("offset:" + inputOffset);
			
			clone.seek(inputOffset);
			
			int len = 0, postingCount = 0, lastDocNo = 0;
			
			//List<Integer> postingList = new ArrayList<Integer>();
		
			try {
				len = clone.readVInt();
				postingCount = clone.readInt();
				lastDocNo = clone.readInt();
				
				int postingRemain = postingCount;
				
				int prevId = -1;
				
				boolean isStorePosition = indexFieldOption.isStorePosition();
				
				for (int i = 0; postingRemain > 0; i++) {
					int docId = -1;
					int readed = clone.readVInt();
					if (prevId >= 0) {
						docId = readed + prevId + 1;
					} else {
						docId = readed;
					}
					
					if(findDocNo == docId) {
						findWord.append("\"").append(string).append("\", ");
					}
					
					int tf = clone.readVInt();
					if (tf > 0 && isStorePosition) {
						
						int prevPosition = -1;
						for (int j = 0; j < tf; j++) {
							int position = 0;
							if (prevPosition >= 0) {
								position = clone.readVInt() + prevPosition + 1;
							} else {
								position = clone.readVInt();
							}
							prevPosition = position;
						}
					}
					//postingList.add(docId);
					
					postingRemain--;
					prevId = docId;
				}
				
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			
			postingInput.close();
			
			//output.println("poosting len : " + len + " postingCount : "
			//		+ postingCount + " / lastDocNo : " + lastDocNo);
			//output.println("postingList:"+postingList);
			if(findDocNo != -1) {
				//output.println("findDocNo:"+findDocNo+" "+(postingList.contains(findDocNo)?"CONTAINS":"NOT"));
			}
		}
		
		output.println("docNo ["+findDocNo+"] has word "+ findWord);
	}
}


