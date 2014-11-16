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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.SettingException;
import org.fastcatsearch.ir.config.DataInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.document.DocumentReader;
import org.fastcatsearch.ir.settings.Schema;
import org.fastcatsearch.ir.settings.SchemaSetting;


public class SearchFieldWriterTest extends TestCase{
	String homePath = "testHome/";
	String collection ="test2";
	String target = homePath+collection+"/data";
	
	public void testMakeIndex() throws SettingException, IOException, IRException{
		SchemaSetting schemaSetting = new SchemaSetting();
		Schema schema = new Schema(schemaSetting);//collection, true);
		File targetDir = new File(target);
		IndexConfig indexConfig = null;
		DocumentReader reader = new DocumentReader(schemaSetting, targetDir);
		AnalyzerPoolManager analyzerPoolManager = null;
		SearchIndexesWriter writer = new SearchIndexesWriter(schema, targetDir, new DataInfo.SegmentInfo(), indexConfig, analyzerPoolManager);
		int documentCount = reader.getDocumentCount();
		for (int docNo = 0; docNo < documentCount; docNo++) {
			Document doc = reader.readDocument(docNo);
			writer.write(doc);
		}
		writer.close();
	}
}
