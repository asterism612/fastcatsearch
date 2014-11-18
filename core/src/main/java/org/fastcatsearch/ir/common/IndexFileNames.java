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

package org.fastcatsearch.ir.common;

import java.io.File;

public class IndexFileNames {
	
	public static final String docStored = "document.stored";
	public static final String docPosition = "document.position";
	public static final String docDeleteSet = "delete.set"; //deleted docs in segment
	public static final String primaryKeyMap = "primarykey.map";
	
	public static final String tempFile = "temp";
	
	public static final String mirrorSync = "mirror.sync";
	
	//삭제문서리스트. datasourcereader에서 내부 문서끼리 충돌된 아이디 리스트가 추가된다.
	public static final String docDeleteList = "delete.list";  
	
	
	public static String getTempFileName(String name){
		return name + "." + tempFile;
	}
	
	//
	// search
	//
	public static String getSearchTempFileName(String name){
		return "search." + name + ".temp";
	}
	public static String getSearchPostingFileName(String name){
		return "search." + name + ".posting";
	}
	public static String getSearchLexiconFileName(String name){
		return "search." + name + ".lexicon";
	}
	public static String getSearchIndexFileName(String name){
		return "search." + name + ".index";
	}
	
	//
	// group
	//
	public static String getGroupIndexFileName(String name){
		return "group." + name + ".index";
	}
	//증분색인시 이전 리비전과의 머징을 위해 필요한 pk파일. group writer에서만 사용된다.
	public static String getGroupKeyMapFileName(String name){
		return "group." + name + ".pk";
	}
	 //그룹순차번호별 key string저장.
	public static String getGroupKeyFileName(String name){
		return "group." + name + ".key";
	}
	
	//
	// field index
	//
	public static String getFieldIndexFileName(String name){
		return "field." + name + ".index";
	}
	
	
	//
	// common
	//
	public static String getSuffixFileName(String name, String suffix){
		return name + "." + suffix;
	}
	public static String getSuffixFileName(String name, String... suffixList){
		for(String suffix : suffixList){
			name += ("."+suffix);
		}
		return name;
	}
	public static String getMultiValueSuffixFileName(String name, String suffix){
		return name + ".mv." + suffix;
	}
	public static String getMultiValueFileName(String name){
		return name + ".mv";
	}
	public static String getPositionFileName(String name){
		return name + ".position";
	}
	
	public static String getIndexFileName(String name){
		return name + ".index";
	}
	
	public static File getRevisionDir(File parent, int revision){
		return new File(parent, Integer.toString(revision));
	}
	
//	public static File getBackupDir(File parent, int revision){
//		return new File(parent, ".bak."+revision);
//	}
}
