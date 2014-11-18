package org.fastcatsearch.ir.config;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.bind.JAXBException;

import org.fastcatsearch.common.io.BytesStreamInput;
import org.fastcatsearch.ir.config.DataInfo.SegmentInfo;
import org.fastcatsearch.ir.io.BytesDataOutput;
import org.fastcatsearch.util.JAXBConfigs;
import org.junit.Test;

public class DataInfoConfigTest {

	@Test
	public void testDataInfoRead() throws IOException, JAXBException {
		String dataInfoXml = "<data-info documents=\"7500\" deletes=\"300\">\n" + 
				"	<segment id=\"0\" base=\"0\" revision=\"2\" documents=\"6000\" deletes=\"250\" createTime=\"2013-06-15 15:30:00\" />\n" + 
				"	<segment id=\"1\" base=\"6000\" revision=\"1\" documents=\"1500\" deletes=\"50\" createTime=\"2013-06-15 16:30:00\" />\n" + 
				"</data-info>";
		InputStream is = new ByteArrayInputStream(dataInfoXml.getBytes());
		DataInfo collectionsConfig = JAXBConfigs.readConfig(is, DataInfo.class);
	}
	
	@Test
	public void testDataInfoWrite() throws JAXBException {
		
		DataInfo dataInfo = new DataInfo();
		dataInfo.setSegmentInfoList(new ArrayList<SegmentInfo>());
		SegmentInfo segmentInfo = new SegmentInfo("0", 0);
//		segmentInfo.updateRevision(new RevisionInfo(0, "1234-5678", 200, 100, 10, 0,Formatter.formatDate(new Date())));
		dataInfo.getSegmentInfoList().add(segmentInfo);
		segmentInfo = new SegmentInfo("1", 1000);
//		segmentInfo.updateRevision(new RevisionInfo(0, "2345-6789", 400, 350, 20, 0, Formatter.formatDate(new Date())));
		dataInfo.getSegmentInfoList().add(segmentInfo);
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		JAXBConfigs.writeRawConfig(os, dataInfo, DataInfo.class);
		System.out.println(os.toString());
		
		
	}

	@Test
	public void testWriteAndRead() throws IOException, JAXBException {
		
		DataInfo dataInfo = new DataInfo();
		
		ByteArrayOutputStream output2 = new ByteArrayOutputStream();
		JAXBConfigs.writeRawConfig(output2, dataInfo, DataInfo.class);
		byte[] arr = output2.toByteArray();
		System.out.println("arr.len = "+arr.length);
		ByteArrayInputStream input2 = new ByteArrayInputStream(arr);
		DataInfo dataInfo22 = JAXBConfigs.readConfig(input2, DataInfo.class);
		
		BytesDataOutput output = new BytesDataOutput();
		JAXBConfigs.writeRawConfig(output, dataInfo, DataInfo.class);
		byte[] arr2 = output.array();
		for(int i =0;i<arr.length; i++){
			assertTrue(arr[i] == arr2[i]);
		}
		
		System.out.println("arr2.len = "+output.position());
		BytesStreamInput input = new BytesStreamInput(arr2, 0, (int) output.position(), true);
		DataInfo dataInfo2 = JAXBConfigs.readConfig(input, DataInfo.class);
	}
	
	@Test
	public void testSegmentInfoWrite() throws JAXBException {
		
		SegmentInfo segmentInfo = new SegmentInfo("5", 200);
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		JAXBConfigs.writeRawConfig(os, segmentInfo, SegmentInfo.class);
		System.out.println(os.toString());
		
		
	}
}
