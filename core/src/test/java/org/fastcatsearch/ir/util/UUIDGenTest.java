package org.fastcatsearch.ir.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 결과 : 4.1GB 해시생성에 51초 소요.
 * */
public class UUIDGenTest {
	protected static Logger logger = LoggerFactory.getLogger(UUIDGenTest.class);
	
	@Test
	public void testGenTime() throws Exception {
		String filePath = "/Users/swsong/Downloads/WindowsTechnicalPreview-x64-EN-US.iso";
		File file = new File(filePath);
		long s = System.currentTimeMillis();
		byte[] hash = createSHA1(file);
		
		logger.info("{}ms, {}({}B)", (System.currentTimeMillis() - s), file.getName(), file.length());
		logger.info("Hash > {}", hash);
	}
	public byte[] createSHA1(File file) throws Exception  {
	    MessageDigest digest = MessageDigest.getInstance("SHA-1");
	    InputStream fis = new FileInputStream(file);
	    int n = 0;
	    byte[] buffer = new byte[8192];
	    while (n != -1) {
	        n = fis.read(buffer);
	        if (n > 0) {
	            digest.update(buffer, 0, n);
	        }
	    }
	    return digest.digest();
	}
}
