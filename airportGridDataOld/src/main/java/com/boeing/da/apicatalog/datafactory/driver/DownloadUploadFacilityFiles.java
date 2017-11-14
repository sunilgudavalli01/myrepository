package com.boeing.da.apicatalog.datafactory.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.net.HttpURLConnection;
public class DownloadUploadFacilityFiles {

	private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
			.getLogger(DownloadUploadFacilityFiles.class);

	private static String filePath = "";

	public DownloadUploadFacilityFiles(String filePath) throws Exception {
		DownloadUploadFacilityFiles.filePath = filePath;
		LOG.info(filePath);
	}

	public void download() throws Exception {
		URL website = new URL(
				"http://uas-faa.opendata.arcgis.com/datasets/6269fe78dc9848d28c6a17065dd56aaf_0.geojson");
		ReadableByteChannel rbc = Channels.newChannel(website.openStream());
		String fileName = "UAS_Facility_Map_Data.geojson";
		File directory = new File(filePath);
		if (!directory.exists()) {
			directory.mkdir();
		}
		FileOutputStream fos = new FileOutputStream(filePath + fileName);
		fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

		rbc.close();
		fos.close();
	}

	public static void uploadFileToBlob(String storageConnectionString,
			String containerName) {
		try {
			File[] files = new File(filePath).listFiles();

			for (File file : files) {
				LOG.info(file.getName());
			}
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(storageConnectionString);

			// Create the blob client.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Retrieve reference to a previously created container.
			CloudBlobContainer container = blobClient
					.getContainerReference(containerName);

			for (File file : files) {
				// Create or overwrite the 'file' blob with contents from a
				// local file.
				CloudBlockBlob blob = container.getBlockBlobReference(file
						.getName());
				blob.upload(new FileInputStream(file), file.length());
			}

		} catch (Exception e) {
			// Output the stack trace.
			e.printStackTrace();
		}

	}
}
