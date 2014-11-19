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

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.config.IndexConfig;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.document.DocumentWriter;
import org.fastcatsearch.ir.settings.Schema;

public class SegmentWriter extends SegmentIndexWriter implements WriteInfoLoggable {

	private int lastDocNo;

	private DocumentWriter documentWriter;

	@Deprecated
	public SegmentWriter(Schema schema, File targetDir, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager) throws IRException {
		this(schema, targetDir, new SegmentInfo(), indexConfig, analyzerPoolManager);
	}

	public SegmentWriter(Schema schema, File targetDir, SegmentInfo segmentInfo, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager) throws IRException {
		this(schema, targetDir, segmentInfo, indexConfig, analyzerPoolManager, null);
	}
	
	public SegmentWriter(Schema schema, File targetDir, SegmentInfo segmentInfo, IndexConfig indexConfig, AnalyzerPoolManager analyzerPoolManager, SelectedIndexList selectedIndexList) throws IRException {
		super(schema, targetDir, segmentInfo, indexConfig, analyzerPoolManager, selectedIndexList);
		try {
			lastDocNo = -1;
			documentWriter = new DocumentWriter(schema.schemaSetting(), targetDir);
		} catch (IOException e) {
			try {
				closeWriter();
			} catch (Exception ignore) {
				// ignore
			}
			throw new IRException(e);
		}
	}


	/**
	 * 색인후 내부 문서번호를 리턴한다.
	 */
	public int addDocument(Document document) throws IRException, IOException {
//		 logger.debug("doc >> {}", document);
		int docNo = documentWriter.write(document);
		super.addDocument(document, docNo);

		lastDocNo = docNo;
		return docNo;
	}

	private void closeWriter() throws Exception {
		boolean errorOccured = false;
		Exception exception = null;
		try {
			documentWriter.close();
		} catch (Exception e) {
			logger.error("문서색인에러", e);
			exception = e;
			errorOccured = true;
		}

		if (errorOccured) {
			throw exception;
		}
	}

	public void close() throws IOException, IRException {
		try {
			closeWriter();
		} catch (Exception e) {
//			File revisionDir = IndexFileNames.getRevisionDir(targetDir, revisionInfo.getId());
			//FileUtils.deleteDirectory(revisionDir);
			FileUtils.forceDelete(targetDir);
			throw new IRException(e);
		} finally {
			super.close();
		}
		

	}

	public void getIndexWriteInfo(IndexWriteInfoList list) {
		documentWriter.getIndexWriteInfo(list);
		super.getIndexWriteInfo(list);
	}
}
