package com.boeing.da.apicatalog.datafactory.driver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.boeing.da.apicatalog.datafactory.loader.AzureDocumentLoader;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class DataProcessDriver {

	private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
			.getLogger(DataProcessDriver.class);

/*	public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=droneappdatastore;"
			+ "AccountKey=r2avTGbzgwJZ21Srh2D0mYIn3Ytvnl3ED/8P3fi85CUW2GVYdGNQaQFedtYLYPEbxFRUbpdbUn00XeUeY6KQ6Q==;";
	public String documentDBHostUrl = "https://drone-app-db.documents.azure.com:443/";
	public String documentDBAccessKey = "KRbVFXDM7ngvnXct086r8qCGGxR0wo0QkWQWlCfX5sOu1dilr37826luxU4q2jpvWG5DtvBmYdVwjD1Kfj9ruQ==";
	public String databaseName = "droneappdata";*/
	
//	public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=droneappdatastore;"
//	+ "AccountKey=r2avTGbzgwJZ21Srh2D0mYIn3Ytvnl3ED/8P3fi85CUW2GVYdGNQaQFedtYLYPEbxFRUbpdbUn00XeUeY6KQ6Q==;";

public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=droneappstorage;"
	+ "AccountKey=kRrLQ8ddwrnsHJJdboHFVrRe73s403fNKcYRnnyLuddoGaa6w56MizulCDMZYOr7Txw88J1gJE4l8KxHvybUKg==;";

//public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=samplstorgeacc;"
//	+ "AccountKey=llXGF3cujc+f8jJWO2OH/hucN84exxQTyt4AZG00XSfBPpjNXLeRLMVWjs1b5M071EFII+QZD7o53WBIBaVQYg==;";

//public static final String containerName = "prisondatastore";
//public static final String containerName = "samplcontainer";
public static final String containerName = "airportgriddatastore";

public String documentDBHostUrl = "https://droneappdata.documents.azure.com:443/";
//public String documentDBHostUrl = "https://sunilsampledb.documents.azure.com:443/";
public String documentDBAccessKey = "dnVQU62JV0sBqhKR9WkHwhzprM1zfjGnHglpbZQDpvjYZq8XxbT61PTwwjrIePCfO65jrliyutm1vtVX5BxMnA==";
//public String documentDBAccessKey = "uzT5cKf3DHbjjoNxxwee6oz9GnTHrObQkWLGKrxGpnBQcitOx4z9j7NZL6nFmDxbG2FJaJZLkCLweJjWxDrLmg==";
public String databaseName = "droneappdata";
//public String databaseName = "gsunilid"; 

//public String collectionName = "prisondata";
//public String collectionName = "samplcollection";
public String collectionName = "airportgriddata1";

	public void downloadFile() {
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			System.out.println("Download Started------------"
					+ dateFormat.format(new Date()));
			LOG.info("Download Started------------"
					+ dateFormat.format(new Date()));

			String filePath = "D:\\Sunil\\DNA\\MLBfromPaul\\JsonFiles4";
			DownloadUploadFacilityFiles d = new DownloadUploadFacilityFiles(
					filePath);
			d.download();

			System.out.println("Download completed------------"
					+ dateFormat.format(new Date()));
			LOG.info("Download completed------------"
					+ dateFormat.format(new Date()));

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	
private static final int BUFFER_SIZE = 4096;
	
	public  void downloadFileUrl(String fileURL, String saveDir)
            throws IOException {
        URL url = new URL(fileURL);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        int responseCode = httpConn.getResponseCode();
 
        // always check HTTP response code first
        if (responseCode == HttpURLConnection.HTTP_OK) {
            String fileName = "";
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
    }


	public void uploadFile() {
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			System.out.println("Upload Started------------"
					+ dateFormat.format(new Date()));
			LOG.info("Upload Started------------"
					+ dateFormat.format(new Date()));

			DownloadUploadFacilityFiles.uploadFileToBlob(
					storageConnectionString, containerName);

			System.out.println("Upload completed-------------"
					+ dateFormat.format(new Date()));
			LOG.info("Upload completed-------------"
					+ dateFormat.format(new Date()));
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	

	public void ingestData() {

		try {

			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			System.out.println("Data Ingestion process started--------------"
					+ dateFormat.format(new Date()));
			LOG.info("Data Ingestion started--------------"
					+ dateFormat.format(new Date()));
//			String inputJsonPath = "D:\\Home\\Jsons\\";
//			String inputJsonPath = "D:\\Boeing\\amarCode9Oct\\airportGrid";
			String inputJsonPath = "D:\\Sunil\\DNA\\MLBfromPaul\\JsonFiles";
//			D:\Sunil\DNA\MLBfromPaul\JsonFiles\airport.geojson
			
			List<Document> documents = fetchDataFromBLOB(inputJsonPath);

			DateFormat dateFormat1 = new SimpleDateFormat("dd/MM/yyyy");
			String ingestedDate = null;
			ingestedDate = dateFormat1.format(new Date());
			List<Document> documents1 = new ArrayList();
			
			for(int i=0;i<documents.size();i++){
				
				Document document = new Document();
				document = documents.get(i);

				JSONObject json = new JSONObject(document.toString());
				JSONArray jsonArray = json.getJSONArray("features");
				
				System.out.println("jsonArray.length()..." + jsonArray.length());
				//64592
				//Forming json
				
				for (int j = 0; j < jsonArray.length(); j++){
					
					
					JSONObject obj = new JSONObject();
					obj = jsonArray.getJSONObject(j);
					JSONObject obj1 = new JSONObject();
					obj1 = obj.getJSONObject("geometry");
					JSONArray coordArray = new JSONArray();
					coordArray = obj1.getJSONArray("coordinates");
					coordArray = (JSONArray) coordArray.get(0);
					JSONArray coordArray1 = new JSONArray();

					for (int k = 0; k < coordArray.length(); k++){
						coordArray1.put(k, coordArray.get(coordArray.length()-k-1));
					}
					
					JSONArray coordArray2 = new JSONArray();
					coordArray2.put(coordArray1);
					
					obj1.remove("coordinates");
					obj1.put("coordinates", coordArray2);
					
					obj.remove("geometry");
					obj.put("geometry", obj1);
					
					String airportID;
					String airportFlag = "N";
					airportID = obj.getJSONObject("properties").getString("AIRPORTID");
					 switch(airportID){
					 
					/* MIA|CVG|PHX|LNK|RNO|SJC|ANC*/
					 case "MIA": 
						 airportFlag="Y" ;break; 
					 case "CVG": 
						 airportFlag="Y" ;break;
					 case "PHX": 
						 airportFlag="Y" ;break;
					 case "LNK": 
						 airportFlag="Y" ;break; 
					 case "RNO": 
						 airportFlag="Y" ;break;
					 case "SJC": 
						 airportFlag="Y" ;break; 
					 case "ANC": 
						 airportFlag="Y" ;break;
//						 |MRI|LHD|ABR|AXN|AMW|IKV|BJI|BRD|BKX
					 case "MRI": 
						 airportFlag="Y" ;break; 
					 case "LHD": 
						 airportFlag="Y" ;break;
					 case "ABR": 
						 airportFlag="Y" ;break; 
					 case "AXN": 
						 airportFlag="Y" ;break;
					 case "AMW": 
						 airportFlag="Y" ;break; 
					 case "IKV": 
						 airportFlag="Y" ;break;
					 case "BJI": 
						 airportFlag="Y" ;break; 
					 case "BRD": 
						 airportFlag="Y" ;break;
					 case "BKX": 
						 airportFlag="Y" ;break; 
//						 |OLU|DVL|DIK|ELO|ESC|FRM|FFM|FOD|CMX|
					 case "OLU": 
						 airportFlag="Y" ;break;
					 case "DVL": 
						 airportFlag="Y" ;break; 
					 case "DIK": 
						 airportFlag="Y" ;break;
					 case "ELO": 
						 airportFlag="Y" ;break; 
					 case "ESC": 
						 airportFlag="Y" ;break;
					 case "FRM": 
						 airportFlag="Y" ;break; 
					 case "FFM": 
						 airportFlag="Y" ;break;
					 case "FOD": 
						 airportFlag="Y" ;break; 
					 case "CMX": 
						 airportFlag="Y" ;break;
//						 HSI|HIB|HON|INL|IMT|IWD|JMS|EAR|MKT|MCW|MHE|
					 case "HSI": 
						 airportFlag="Y" ;break; 
					 case "HIB": 
						 airportFlag="Y" ;break;
					 case "HON": 
						 airportFlag="Y" ;break; 
					 case "INL": 
						 airportFlag="Y" ;break;
					 case "IMT": 
						 airportFlag="Y" ;break; 
					 case "IWD": 
						 airportFlag="Y" ;break;
					 case "JMS": 
						 airportFlag="Y" ;break; 
					 case "EAR": 
						 airportFlag="Y" ;break;
					 case "MKT": 
						 airportFlag="Y" ;break; 
					 case "MCW": 
						 airportFlag="Y" ;break;
					 case "MHE": 
						 airportFlag="Y" ;break; 
//						 OFK|OSC|PLN|PIR|RWF|RHI|CIU|SPW|SLB|TVF|ATY|OTG|YKN
					 case "OFK": 
						 airportFlag="Y" ;break;
					 case "OSC": 
						 airportFlag="Y" ;break; 
					 case "PLN": 
						 airportFlag="Y" ;break;
					 case "PIR": 
						 airportFlag="Y" ;break; 
					 case "RWF": 
						 airportFlag="Y" ;break;
					 case "RHI": 
						 airportFlag="Y" ;break; 
					 case "CIU": 
						 airportFlag="Y" ;break; 
					 case "SPW": 
						 airportFlag="Y" ;break;
					 case "SLB": 
						 airportFlag="Y" ;break; 
					 case "TVF": 
						 airportFlag="Y" ;break;
					 case "ATY": 
						 airportFlag="Y" ;break;
					 case "OTG": 
						 airportFlag="Y" ;break;
					 case "YKN": 
						 airportFlag="Y" ;break; 
					
					
					 }
					
					/*if(airportID.matches("MIA|CVG|PHX|LNK|RNO|SJC|ANC|MRI|LHD|ABR|AXN|AMW|IKV|BJI|BRD|BKX|OLU|DVL|DIK|ELO|ESC|FRM|FFM|FOD|CMX|HSI|HIB|HON|INL|IMT|IWD|JMS|EAR|MKT|MCW|MHE|OFK|OSC|PLN|PIR|RWF|RHI|CIU|SPW|SLB|TVF|ATY|OTG|YKN")){
						obj.put("airportFlag", "Y");
					}
					else {
						obj.put("airportFlag", "N");
					}*/
					obj.put("airportFlag", airportFlag);
					obj.put("ingestedDate", ingestedDate);
					
					Document returnDoc = new Document();
					returnDoc = new Document(obj.toString());
					documents1.add(returnDoc);
					
				}

				System.out.println("Documents size "+documents1.size());
				
			}
			
			
//			System.out.println("starting DB load process"
//					+ dateFormat.format(new Date()));
//			LOG.info("starting DB load process" + dateFormat.format(new Date()));
//
//			AzureDocumentLoader loader = new AzureDocumentLoader(
//					documentDBHostUrl, documentDBAccessKey, databaseName,
//					collectionName);
//			loader.loadDocuments(collectionName, documents1);
//			
//
//			System.out.println("Data Ingestion completed------------"+documents1.size()
//					+ dateFormat.format(new Date()));
//			LOG.info("Data Ingestion completed------------"+documents1.size()
//					+ dateFormat.format(new Date()));
			
//			loader.deleteOldDocuments(collectionName, ingestedDate);

		} catch (Exception ex) {
			ex.printStackTrace();

		}

	}

	public List<Document> fetchDataFromBLOB(String inputJsonPath)
			throws StorageException, IOException {

		String fileName = "";
		//String collectionName = "";
		String fileContent = "";
		
		List<Document> documents = null;
		try {

			//List<Document> documents1 = null;
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(storageConnectionString);
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			CloudBlobContainer container = blobClient
					.getContainerReference(containerName);
			container.createIfNotExists();
			File directory = new File(inputJsonPath);
			if (!directory.exists()) {
				directory.mkdir();
			}
			for (ListBlobItem blobItem : container.listBlobs()) {
				 documents = new ArrayList();
				fileName = ((CloudBlockBlob) blobItem).getName();
				String[] fileElements = fileName.split("\\.");
				System.out.println("file elements" +fileElements);
				

				System.out.println(blobItem.getUri());
				System.out.println(fileName);
				LOG.info(blobItem.getUri());
				LOG.info(fileName);

				CloudBlockBlob blob = container.getBlockBlobReference(fileName);

				System.out.println("Fetching data from Blob");
				LOG.info("Fetching data from Blob");
				fileContent = blob.downloadText();
				JSONObject objContent = new JSONObject(fileContent);
				Document d = new Document(objContent.toString());
				documents.add(d);
				
				DateFormat dateFormat1 = new SimpleDateFormat("dd/MM/yyyy");
				String ingestedDate = null;
				ingestedDate = dateFormat1.format(new Date());
				List<Document> documents1 = new ArrayList();
				
				System.out.println("doc size is:::"+documents.size()+":for file:"+fileName);
				
				for(int i=0;i<documents.size();i++){
					
					Document document = new Document();
					document = documents.get(i);

					JSONObject json = new JSONObject(document.toString());
					JSONArray jsonArray = json.getJSONArray("features");
					
					System.out.println("jsonArray.length()..." + jsonArray.length());
					//64592
					//Forming json
					
					for (int j = 0; j < jsonArray.length(); j++){
						
						
						JSONObject obj = new JSONObject();
						obj = jsonArray.getJSONObject(j);
						JSONObject obj1 = new JSONObject();
						obj1 = obj.getJSONObject("geometry");
						JSONArray coordArray = new JSONArray();
						coordArray = obj1.getJSONArray("coordinates");
						coordArray = (JSONArray) coordArray.get(0);
						JSONArray coordArray1 = new JSONArray();

						for (int k = 0; k < coordArray.length(); k++){
							coordArray1.put(k, coordArray.get(coordArray.length()-k-1));
						}
						
						JSONArray coordArray2 = new JSONArray();
						coordArray2.put(coordArray1);
						
						obj1.remove("coordinates");
						obj1.put("coordinates", coordArray2);
						
						obj.remove("geometry");
						obj.put("geometry", obj1);
						
						String airportID;
						String airportFlag = "N";
						airportID = obj.getJSONObject("properties").getString("AIRPORTID");
						 switch(airportID){
						 
						/* MIA|CVG|PHX|LNK|RNO|SJC|ANC*/
						 case "MIA": 
							 airportFlag="Y" ;break; 
						 case "CVG": 
							 airportFlag="Y" ;break;
						 case "PHX": 
							 airportFlag="Y" ;break;
						 case "LNK": 
							 airportFlag="Y" ;break; 
						 case "RNO": 
							 airportFlag="Y" ;break;
						 case "SJC": 
							 airportFlag="Y" ;break; 
						 case "ANC": 
							 airportFlag="Y" ;break;
//							 |MRI|LHD|ABR|AXN|AMW|IKV|BJI|BRD|BKX
						 case "MRI": 
							 airportFlag="Y" ;break; 
						 case "LHD": 
							 airportFlag="Y" ;break;
						 case "ABR": 
							 airportFlag="Y" ;break; 
						 case "AXN": 
							 airportFlag="Y" ;break;
						 case "AMW": 
							 airportFlag="Y" ;break; 
						 case "IKV": 
							 airportFlag="Y" ;break;
						 case "BJI": 
							 airportFlag="Y" ;break; 
						 case "BRD": 
							 airportFlag="Y" ;break;
						 case "BKX": 
							 airportFlag="Y" ;break; 
//							 |OLU|DVL|DIK|ELO|ESC|FRM|FFM|FOD|CMX|
						 case "OLU": 
							 airportFlag="Y" ;break;
						 case "DVL": 
							 airportFlag="Y" ;break; 
						 case "DIK": 
							 airportFlag="Y" ;break;
						 case "ELO": 
							 airportFlag="Y" ;break; 
						 case "ESC": 
							 airportFlag="Y" ;break;
						 case "FRM": 
							 airportFlag="Y" ;break; 
						 case "FFM": 
							 airportFlag="Y" ;break;
						 case "FOD": 
							 airportFlag="Y" ;break; 
						 case "CMX": 
							 airportFlag="Y" ;break;
//							 HSI|HIB|HON|INL|IMT|IWD|JMS|EAR|MKT|MCW|MHE|
						 case "HSI": 
							 airportFlag="Y" ;break; 
						 case "HIB": 
							 airportFlag="Y" ;break;
						 case "HON": 
							 airportFlag="Y" ;break; 
						 case "INL": 
							 airportFlag="Y" ;break;
						 case "IMT": 
							 airportFlag="Y" ;break; 
						 case "IWD": 
							 airportFlag="Y" ;break;
						 case "JMS": 
							 airportFlag="Y" ;break; 
						 case "EAR": 
							 airportFlag="Y" ;break;
						 case "MKT": 
							 airportFlag="Y" ;break; 
						 case "MCW": 
							 airportFlag="Y" ;break;
						 case "MHE": 
							 airportFlag="Y" ;break; 
//							 OFK|OSC|PLN|PIR|RWF|RHI|CIU|SPW|SLB|TVF|ATY|OTG|YKN
						 case "OFK": 
							 airportFlag="Y" ;break;
						 case "OSC": 
							 airportFlag="Y" ;break; 
						 case "PLN": 
							 airportFlag="Y" ;break;
						 case "PIR": 
							 airportFlag="Y" ;break; 
						 case "RWF": 
							 airportFlag="Y" ;break;
						 case "RHI": 
							 airportFlag="Y" ;break; 
						 case "CIU": 
							 airportFlag="Y" ;break; 
						 case "SPW": 
							 airportFlag="Y" ;break;
						 case "SLB": 
							 airportFlag="Y" ;break; 
						 case "TVF": 
							 airportFlag="Y" ;break;
						 case "ATY": 
							 airportFlag="Y" ;break;
						 case "OTG": 
							 airportFlag="Y" ;break;
						 case "YKN": 
							 airportFlag="Y" ;break; 
						
						
						 }
						
						/*if(airportID.matches("MIA|CVG|PHX|LNK|RNO|SJC|ANC|MRI|LHD|ABR|AXN|AMW|IKV|BJI|BRD|BKX|OLU|DVL|DIK|ELO|ESC|FRM|FFM|FOD|CMX|HSI|HIB|HON|INL|IMT|IWD|JMS|EAR|MKT|MCW|MHE|OFK|OSC|PLN|PIR|RWF|RHI|CIU|SPW|SLB|TVF|ATY|OTG|YKN")){
							obj.put("airportFlag", "Y");
						}
						else {
							obj.put("airportFlag", "N");
						}*/
						obj.put("airportFlag", airportFlag);
						obj.put("ingestedDate", ingestedDate);
						obj.put("RowNumber", i);
						
						Document returnDoc = new Document();
						returnDoc = new Document(obj.toString());
						documents1.add(returnDoc);
						
					}

				
				
			}
				AzureDocumentLoader loader = new AzureDocumentLoader(
						documentDBHostUrl, documentDBAccessKey, databaseName,
						collectionName);
				loader.loadDocuments(collectionName, documents1);
				

				System.out.println("Data Ingestion completed------------:for file:"+fileName);
				loader.deleteOldDocuments(collectionName, ingestedDate);				

		}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return documents;
		
	}
	public static void uploadFileToBlob(String filePath) {
		try {
			File[] files = new File(filePath).listFiles();

			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(storageConnectionString);

			// Create the blob client.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Retrieve reference to a previously created container.
			CloudBlobContainer container = blobClient
					.getContainerReference(containerName);
System.out.println("no. of files::"+files.length);
			for (File file : files) {
				// Create or overwrite the 'file' blob with contents from a
				// local file.
				System.out.println(file.getName());
				CloudBlockBlob blob = container.getBlockBlobReference(file
						.getName());
				blob.upload(new FileInputStream(file), file.length());
				System.out.println("uploaded ");
			}

		} catch (Exception e) {
			// Output the stack trace.
			e.printStackTrace();
		}

	}
	
	
	
}
