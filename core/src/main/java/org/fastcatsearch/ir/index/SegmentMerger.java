package org.fastcatsearch.ir.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.ir.common.IRException;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.document.Document;
import org.fastcatsearch.ir.document.DocumentReader;
import org.fastcatsearch.ir.document.DocumentWriter;
import org.fastcatsearch.ir.io.BitSet;
import org.fastcatsearch.ir.search.FieldIndexReader;
import org.fastcatsearch.ir.search.SegmentReader;
import org.fastcatsearch.ir.settings.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentMerger {
	protected static final Logger logger = LoggerFactory
			.getLogger(SegmentMerger.class);

	private Schema schema;
	private File indexDir;

	public SegmentMerger(Schema schema, File indexDir) {
		this.schema = schema;
		this.indexDir = indexDir;
	}

	public SegmentInfo merge(List<SegmentInfo> segmentInfoList) {
		if (segmentInfoList.size() < 2) {
			// 머징할 것이 없다.
			return null;
		}

		try {

			List<SegmentReader> segmentReaderList = new ArrayList<SegmentReader>();

			// 세그먼트를 모두 연다.
			for (SegmentInfo segmentInfo : segmentInfoList) {
				File segmentDir = new File(indexDir, segmentInfo.getId());
				segmentReaderList.add(new SegmentReader(segmentInfo, schema,
						segmentDir, null));
			}

			/*
			 * 1. Document머징.
			 */
			DocumentWriter writer = new DocumentWriter(schema.schemaSetting(), indexDir);
			int segmentNum = 0;
			int totalDocumentCount = 0;
			for (SegmentReader segmentReader : segmentReaderList) {
				BitSet deleteSet = segmentReader.deleteSet();
				DocumentReader documentReader = null;
				try {
					documentReader = segmentReader.newDocumentReader();
					int docCount = documentReader.getDocumentCount();
					int baseNumber = documentReader.getBaseNumber();
					int count = 0;
					for (int i = 0; i < docCount; i++) {
						if (!deleteSet.isSet(i)) {
							Document document = documentReader.readDocument(i);
							writer.write(document);
							count++;
						}

					}
					
					totalDocumentCount += count;
				} finally {
					if (documentReader != null) {
						documentReader.close();
					}
				}
				segmentNum++;
				
			}

			
			/*
			 * 2. search index  
			 * */
			
			
			
			/*
			 * 3. field index 
			 * */
			
			
			FieldIndexesMerger m = new FieldIndexesMerger(schema, indexDir, segmentReaderList);
			m.merge();
			
//			FieldIndexWriter fieldIndexWriter = new FieldIndexesWriter(schema, dir, segmentInfo);
//			fieldIndexWriter.write(document);
			
//			FieldIndexReader fieldIndexReader = new FieldIndexReader(fieldIndexSetting, fieldSettingMap, dir);
			
			
			/*
			 * 4. group index
			 */
			
			
			
			
		} catch (IOException e) {
			logger.error("", e);
		} catch (IRException e) {
			logger.error("", e);
		}

		return null;
	}

}
