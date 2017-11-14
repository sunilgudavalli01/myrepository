package com.boeing.da.apicatalog.datafactory.driver;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;

public class DataIngestionDriver {

	public static void main(String[] args) {
		java.lang.System.setProperty("java.net.preferIPv4Stack", "true");
		DataProcessDriver  dataProcessDriver=new DataProcessDriver();
		Splitter split = new Splitter();
		String url = "http://uas-faa.opendata.arcgis.com/datasets/6269fe78dc9848d28c6a17065dd56aaf_0.geojson";
		String directoryName = "D:\\Sunil\\DNA\\MLBfromPaul\\airport1";
		String fileName = "UAS_Facility_Map_Data.geojson";
//		try {
//			File directory = new File(directoryName+ File.separator +"splitFolder");
//			if (!directory.exists()) {
//				directory.mkdir();
//			}
//			dataProcessDriver.downloadFileUrl(url,directoryName);
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		try {
//			split.fileSpilter(fileName, 5000, directoryName);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		dataProcessDriver.uploadFileToBlob(directoryName+ File.separator +"splitFolder");
//		dataProcessDriver.uploadFileToBlob("D:\\Sunil\\DNA\\MLBfromPaul\\JsonFiles5");
		dataProcessDriver.ingestData();
		System.out.println("Completed Successfully");
		
	}

}
