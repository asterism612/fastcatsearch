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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.SettingException;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.field.AStringField;
import org.fastcatsearch.ir.field.AStringMvField;
import org.fastcatsearch.ir.field.DatetimeField;
import org.fastcatsearch.ir.field.FieldDataParseException;
import org.fastcatsearch.ir.field.IntField;
import org.fastcatsearch.ir.field.IntMvField;
import org.fastcatsearch.ir.field.LongField;
import org.fastcatsearch.ir.field.UStringField;
import org.fastcatsearch.ir.field.UStringMvField;
import org.fastcatsearch.ir.settings.AnalyzerSetting;
import org.fastcatsearch.ir.settings.FieldIndexSetting;
import org.fastcatsearch.ir.settings.FieldSetting;
import org.fastcatsearch.ir.settings.GroupIndexSetting;
import org.fastcatsearch.ir.settings.IndexRefSetting;
import org.fastcatsearch.ir.settings.IndexSetting;
import org.fastcatsearch.ir.settings.PrimaryKeySetting;
import org.fastcatsearch.ir.settings.SchemaSetting;
import org.junit.Test;


public class DocumentWriteReadTest extends TestCase{
	
	
	public void testWrite() throws IRException, IOException, SettingException{
		File targetDir = new File("temp/");
		
		
		SchemaSetting schemaSetting = createSchemaSetting();
		IndexConfig indexConfig = createIndexConfig();
		DocumentWriter dw = new DocumentWriter(schemaSetting, targetDir, indexConfig);
		
		Document document = new Document(6);
		document.add(new LongField("id", "100"));
		document.add(new UStringField("title", "안녕하세요."));
		document.add(new IntField("price", "1500"));
		document.add(new AStringField("category", "computer"));
		AStringMvField f = new AStringMvField("category");
		f.addValue("computer");
		f.addValue("elec");
		f.addValue("blog");
		document.add(f);
		document.add(new DatetimeField("regdate", "2013-06-13 12:15:00"));
		
		dw.write(document);
		
		dw.close();
		
	}
	
	public void testWriteAndRead() throws IRException, IOException, SettingException{
		File targetDir = new File("temp/");
		
		SchemaSetting schemaSetting = createSchemaSetting();
//		schema.getFieldSettingList().get(2).setStore(false); //field-2는 저장하지 않음.
		IndexConfig indexConfig = createIndexConfig();
		DocumentWriter dw = new DocumentWriter(schemaSetting, targetDir, indexConfig);
		
		Document document = new Document(6);
		document.add(new LongField("id", "100"));
		document.add(new UStringField("title", "안녕하세요."));
		document.add(new IntField("price", "1500"));
		document.add(new AStringField("category", "computer"));
		AStringMvField f = new AStringMvField("tags");
		f.addValue("computer");
		f.addValue("elec");
		f.addValue("blog");
		document.add(f);
		document.add(new DatetimeField("regdate", "2013-06-13 12:15:00"));
		dw.write(document);
		
		document = new Document(6);
		document.add(new LongField("id", "101"));
		document.add(new UStringField("title", "안녕하세요2."));
		document.add(new IntField("price", "2500"));
		document.add(new AStringField("category", "acc"));
		f = new AStringMvField("tags");
		f.addValue("computer2");
		f.addValue("elec2");
		f.addValue("blog2");
		document.add(f);
		document.add(new DatetimeField("regdate", "2013-06-15 12:15:00"));
		dw.write(document);
		
		
		dw.close();
		
		
		DocumentReader reader = new DocumentReader(schemaSetting, targetDir);
		Document actualDocument = reader.readDocument(0);
		System.out.println(actualDocument);
		actualDocument = reader.readDocument(1);
		System.out.println(actualDocument);
		
		reader.close();
		
		//FileUtils.deleteDirectory(targetDir);
		FileUtils.forceDelete(targetDir);
	}
	
	public void testWriteAndReadMultiField() throws IRException, IOException, SettingException{
		File targetDir = new File("temp/");
		
		SchemaSetting schemaSetting = createSchemaSetting();
		schemaSetting.getFieldSettingList().get(1).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(2).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(3).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(4).setMultiValue(true);
		
		IndexConfig indexConfig = createIndexConfig();
		DocumentWriter dw = new DocumentWriter(schemaSetting, targetDir, indexConfig);
		
		Document document = new Document(6);
		document.add(new LongField("id", "100"));
		UStringMvField f2 = new UStringMvField("title", 4);
		f2.addValue("안녕하세요1");
		f2.addValue("안녕하세요2");
		f2.addValue("안녕하세요3");
		document.add(f2);
		IntMvField f3 = new IntMvField("price");
		f3.addValue("2500");
		f3.addValue("3500");
		f3.addValue("4700");
		document.add(f3);
		AStringMvField f4 = new AStringMvField("category");
		f4.addValue("acc");
		f4.addValue("acc2");
		f4.addValue("acc3");
		document.add(f4);
		AStringMvField f5 = new AStringMvField("tags");
		f5.addValue("computer");
		f5.addValue("elec");
		f5.addValue("blog");
		document.add(f5);
		document.add(new DatetimeField("regdate", "2013-06-13 12:15:00"));
		
		dw.write(document);
		
		dw.close();
		
		
		DocumentReader reader = new DocumentReader(schemaSetting, targetDir);
		Document actualDocument = reader.readDocument(0);
		
		System.out.println(actualDocument);
		
		//FileUtils.deleteDirectory(targetDir);
		FileUtils.forceDelete(targetDir);
	}
	
	
	public void testWriteAndReadMultiFieldLarge() throws IRException, IOException, SettingException{
		File targetDir = new File("temp/");
		
		SchemaSetting schemaSetting = createSchemaSetting();
		schemaSetting.getFieldSettingList().get(1).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(2).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(3).setMultiValue(true);
		schemaSetting.getFieldSettingList().get(4).setMultiValue(true);
		
		IndexConfig indexConfig = createIndexConfig();
		DocumentWriter dw = new DocumentWriter(schemaSetting, targetDir, indexConfig);
		
		int count = 300;
		for (int i = 0; i < count; i++) {
			Document document = createDocument(i);
			dw.write(document);
			if(i % 100 == 0){
				System.out.println("wrote "+i+"...");
			}
		}
		
		dw.close();
		
		
		DocumentReader reader = new DocumentReader(schemaSetting, targetDir);
		for (int i = 0; i < count; i++) {
			Document actualDocument = reader.readDocument(i);
			
			System.out.println("doc"+i);
			System.out.println(actualDocument);
		}
		
		//FileUtils.deleteDirectory(targetDir);
		FileUtils.forceDelete(targetDir);
	}
	
	
	Random r = new Random(System.currentTimeMillis());
	
	private String getUString(int size){
		int ch = r.nextInt(0xD7A3 - 0xAC00) + 0xAC00;
		
		String str = "";
		for (int i = 0; i < 30; i++) {
			str += (char) ch;
		}
		
		return str;
	}
	
	private String getAString(int size){
		int ch = r.nextInt('Z' - 'A') + 'A';
		
		String str = "";
		for (int i = 0; i < 30; i++) {
			str += (char) ch;
		}
		
		return str;
	}
	
	@Test
	public void test1(){
		for (int i = 0; i < 100000; i++) {
			System.out.println(getUString(30));
			System.out.println(getAString(30));
		}
	}
	
	private Document createDocument(int i) throws FieldDataParseException{
		Document document = new Document(6);
		document.add(new LongField("id", Integer.toString(i)));
		UStringMvField f2 = new UStringMvField("title");
		for (int j = 0; j < 1000; j++) {
			f2.addValue(getUString(10));
		}
		document.add(f2);
		
		IntMvField f3 = new IntMvField("price");
		for (int j = 0; j < 1000; j++) {
			f3.addValue(Integer.toString(r.nextInt()));
		}
		document.add(f3);
		
		AStringMvField f4 = new AStringMvField("category");
		for (int j = 0; j < 1000; j++) {
			f4.addValue(getAString(10));
		}
		document.add(f4);
		
		AStringMvField f5 = new AStringMvField("tags");
		for (int j = 0; j < 1000; j++) {
			f5.addValue(getAString(10));
		}
		document.add(f5);
		
		document.add(new DatetimeField("regdate", "2013-06-13 12:15:00"));
		
		return document;
	}
	
	public SchemaSetting createSchemaSetting(){
		SchemaSetting setting = new SchemaSetting();
		
		//add fieldsetting
		List<FieldSetting> fieldSettingList = new ArrayList<FieldSetting>();
		FieldSetting fieldSetting = new FieldSetting("id", "글아이디",  FieldSetting.Type.LONG);
		fieldSettingList.add(fieldSetting);
		fieldSetting = new FieldSetting("title", "제목",  FieldSetting.Type.STRING);
		fieldSetting.setSize(30);
		fieldSettingList.add(fieldSetting);
		fieldSetting = new FieldSetting("price", "가격",  FieldSetting.Type.INT);
		fieldSettingList.add(fieldSetting);
		fieldSetting = new FieldSetting("category", "카테고리",  FieldSetting.Type.ASTRING);
		fieldSettingList.add(fieldSetting);
		fieldSetting = new FieldSetting("tags", "태그",  FieldSetting.Type.ASTRING);
		fieldSetting.setMultiValue(true);
		fieldSettingList.add(fieldSetting);
		fieldSetting = new FieldSetting("regdate", "등록일",  FieldSetting.Type.DATETIME);
		fieldSettingList.add(fieldSetting);
		
		setting.setFieldSettingList(fieldSettingList);
		
		
		PrimaryKeySetting primaryKeySetting = new PrimaryKeySetting("id");
		setting.setPrimaryKeySetting(primaryKeySetting);
//		setting.getPrimaryKeySettingList().add(new PrimaryKeySetting("regdate"));
		
		setting.setIndexSettingList(new ArrayList<IndexSetting>());
		IndexSetting indexSetting = new IndexSetting("title_index", "korean");
		indexSetting.setFieldList(new ArrayList<IndexRefSetting>());
		indexSetting.getFieldList().add(new IndexRefSetting("title", "korean"));
		setting.getIndexSettingList().add(indexSetting);
		
		List<FieldIndexSetting> fieldIndexSettingList = new ArrayList<FieldIndexSetting>();
		FieldIndexSetting fieldIndexSetting = new FieldIndexSetting("title", "title field index", "title");
		fieldIndexSettingList.add(fieldIndexSetting);
		fieldIndexSetting = new FieldIndexSetting("tags", "title tags field index", "tags");
		fieldIndexSettingList.add(fieldIndexSetting);
		setting.setFieldIndexSettingList(fieldIndexSettingList);
		
		
		List<GroupIndexSetting> groupSettingList = new ArrayList<GroupIndexSetting>();
		GroupIndexSetting groupIndexSetting = new GroupIndexSetting("category", "category_group", "category");
		groupSettingList.add(groupIndexSetting);
		setting.setGroupIndexSettingList(groupSettingList);
		
		List<AnalyzerSetting> analyzerSettingList = new ArrayList<AnalyzerSetting>();
		analyzerSettingList.add(new AnalyzerSetting("korean_index","korean_index", 10, 100, "com.fastcatsearch.plugin.analysis.korean.StandardKoreanAnalyzer"));
		analyzerSettingList.add(new AnalyzerSetting("korean_query","korean_query", 10, 100, "com.fastcatsearch.plugin.analysis.korean.StandardKoreanAnalyzer"));
		setting.setAnalyzerSettingList(analyzerSettingList);
		
		return setting;
	}
	
	public IndexConfig createIndexConfig(){
		IndexConfig config = new IndexConfig();
		config.setIndexTermInterval(64);
		config.setIndexWorkBucketSize(256);
		config.setIndexWorkMemorySize(32 * 1024 * 1024);
		config.setPkBucketSize(64 * 1024);
		config.setPkTermInterval(64);
		
		return config;
	}
}
