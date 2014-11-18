/*
 * Copyright (c) 2013 Websquared, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *     swsong - initial API and implementation
 */

package org.fastcatsearch.job.indexing;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.ir.common.IndexFileNames;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.BufferedFileInput;
import org.fastcatsearch.ir.io.BufferedFileOutput;
import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;


public class MirrorIndexMaker {

	public MirrorIndexInfo make(SegmentInfo segmentInfo, File newRevDir, boolean isAppended) {
//		File curRevDir = new File(segmentInfo.getSegmentDir(), segmentInfo.getLastRevision()+"");
		File newSegmentDir = null;//segmentInfo.getSegmentDir();
		
		File mirrorDir = new File(newSegmentDir.getParentFile(), "mirror");
		//상위의 mirror디렉토리에 미러색인파일을 기록한다.
		if(mirrorDir.exists()){
			try {
				//FileUtils.deleteDirectory(mirrorDir);
				FileUtils.forceDelete(mirrorDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		mirrorDir.mkdir();
		
		int docCount = segmentInfo.getDocumentCount();
		
		try {
			//1. doc문서파일.
			byte[] buf = new byte[8 * 1024];
			DataInput fileInput = new BufferedFileInput(newRevDir, IndexFileNames.docPosition);
			DataOutput fileOutput = new BufferedFileOutput(mirrorDir, IndexFileNames.docPosition);
//			fileInput.seek(segmentInfo.docPositionFilesSize);
			int nread = 0;
//			while((nread = fileInput.readBytes(buf, 0, buf.length)) > 0){
//				fileOutput.writeBytes(buf, 0, nread);
//			}
			fileOutput.close();

			//2. 
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
