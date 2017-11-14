package com.boeing.da.apicatalog.datafactory.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

//import org.json.JSONException;
//import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.HttpURLConnection;

public class Splitter {

	private static final int BUFFER_SIZE = 4096;
	
	public static String downloadFile(String fileURL, String saveDir)
            throws IOException {
        URL url = new URL(fileURL);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        int responseCode = httpConn.getResponseCode();
        String fileName = "";
 
        // always check HTTP response code first
        if (responseCode == HttpURLConnection.HTTP_OK) {
            
            String disposition = httpConn.getHeaderField("Content-Disposition");
            String contentType = httpConn.getContentType();
            int contentLength = httpConn.getContentLength();
 
            if (disposition != null) {
                // extracts file name from header field
                int index = disposition.indexOf("filename=");
                if (index > 0) {
                    fileName = disposition.substring(index + 10,
                            disposition.length() - 1);
                }
            } else {
                // extracts file name from URL
                fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1,
                        fileURL.length());
            }
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            System.out.println("Download Started------------"
					+ dateFormat.format(new Date()));
            System.out.println("Content-Type = " + contentType);
            System.out.println("Content-Disposition = " + disposition);
            if(disposition!=null && disposition.contains("filename=")){
            	fileName = disposition.substring(disposition.lastIndexOf("=")+1,disposition.length());
            }
            System.out.println("Content-Length = " + contentLength);
            System.out.println("fileName = " + fileName);
 
            // opens input stream from the HTTP connection
            InputStream inputStream = httpConn.getInputStream();
            String saveFilePath = saveDir + File.separator + fileName;
             
            // opens an output stream to save into file
            FileOutputStream outputStream = new FileOutputStream(saveFilePath);
 
            int bytesRead = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
 
            outputStream.close();
            inputStream.close();
 
            System.out.println("File downloaded"+ dateFormat.format(new Date()));
        } else {
            System.out.println("No file to download. Server replied HTTP code: " + responseCode);
        }
        httpConn.disconnect();
        return fileName;
    }
	public static void fileSpilter(String fileName, int noOfObj,String readDir) throws Exception{
		
		JSONParser parser = new JSONParser();
		Object obj;
		
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            System.out.println("splitting files started------------"
					+ dateFormat.format(new Date()));
			obj = parser.parse(new FileReader(readDir + File.separator + fileName));
			JSONObject jsonObject = (JSONObject) obj;
			JSONArray jsonArray = (JSONArray)  jsonObject.get("features");
			int size = jsonArray.size();
			int div = size/noOfObj;
			System.out.println("size of Array::"+size);
			System.out.println("size of div::"+div);
			Iterator<JSONObject> iterator = jsonArray.iterator();
			JSONObject tempFileObj = null;
			int i = 0;
			int j =1;
			JSONArray jsonSUbArray = new JSONArray();
			while (iterator.hasNext()) {
				JSONObject tempObj = new JSONObject();
				tempObj =(JSONObject) iterator.next();
				i++;
//				System.out.println("i before if:"+i);
				if(i<=(noOfObj*div)){
				jsonSUbArray.add(tempObj);
				
				
				if(i%noOfObj==0){
					System.out.println(j+" th file  split started at:::"
							+ dateFormat.format(new Date())+"::i ="+i+"::noOfObj*j::"+(noOfObj*j));
					tempFileObj = new JSONObject();
					tempFileObj.put("features", jsonSUbArray);
					flushFile(readDir,tempFileObj,j,fileName);
					jsonSUbArray = new JSONArray();
					j++;
					
				}
				}
				else{
					
					jsonSUbArray.add(tempObj);
					if(i==size){
					System.out.println(j+" th file  split started at:::"
							+ dateFormat.format(new Date())+"::i ="+i);
					tempFileObj = new JSONObject();
					tempFileObj.put("features", jsonSUbArray);
					flushFile(readDir,tempFileObj,j,fileName);
					}
				}
				
				
				
			}
			
			
			
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
    public static void flushFile(String saveDir,JSONObject obj, int fileNum,String fileName) throws Exception {
    	
        try (FileOutputStream output = new FileOutputStream(saveDir + File.separator +"splitFolder"+ File.separator + fileName + fileNum + ".geojson");
             Writer writer = new OutputStreamWriter(output, "UTF-8")) {
        	System.out.println("in flush() for fileNum::"+fileNum);
        	obj.writeJSONString(writer);
//             obj.write(writer);
        }
    }
	  
	 	  
	  
	  
	/*public static void main(String[] args)  throws Exception  {
//		JSONObject json = readJsonFromUrl("http://api.sportradar.us/nascar-t3/mc/2017/races/schedule.json?api_key=awfbgs9x75x53k325en38df4",
//				"D:\\Sunil\\DNA\\schlnprisndata\\PrisonDataFromServer2\\","me.json");
//	    System.out.println(json.toString());
		String fileName = "";
		String directoryName = "D:\\Sunil\\DNA\\MLBfromPaul\\JsonFiles3";
		File directory = new File(directoryName);
		if (!directory.exists()) {
			directory.mkdir();
		}
//		fileName=downloadFile("http://uas-faa.opendata.arcgis.com/datasets/6269fe78dc9848d28c6a17065dd56aaf_0.geojson ",directoryName);
		fileName = "UAS_Facility_Map_Data.geojson";
		fileSpilter(fileName, 100, directoryName);
		
//	    System.out.println(json.get("coordinates"));	
	}
	
//	 public static JSONObject readJsonFromUrl(String url,String directoryName,String fileName) throws IOException, JSONException {
//		    InputStream is = new URL(url).openStream();
//		    try {
//		      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
//		      
//		      StringBuilder sb = new StringBuilder();
//			    int cp;
//			    while ((cp = rd.read()) != -1) {
//			      sb.append((char) cp);
//			    }
//		      
//		      String jsonText = sb.toString();
//		      File directory = new File(directoryName);
//				System.out.println(directory.exists());
//				if (!directory.exists()) {
//					directory.mkdir();
//				}
//				try {
//					File file = new File(directoryName+fileName);
//					FileWriter fileWriter = new FileWriter(file);
//					fileWriter.write(jsonText);
//					fileWriter.flush();
//					fileWriter.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//		      JSONObject json = new JSONObject(jsonText);
//		      return json;
//		    } finally {
//		      is.close();
//		    }
//		  }
		  
*/		  
}
