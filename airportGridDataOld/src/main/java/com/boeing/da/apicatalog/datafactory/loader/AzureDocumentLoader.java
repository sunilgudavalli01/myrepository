package com.boeing.da.apicatalog.datafactory.loader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


//import org.apache.commons.lang3.StringUtils;
//import org.apache.http.HttpHost;
import org.json.JSONException;

import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;

/**
 * This class defines methods to load a list of Azure Documents to DocumentDB
 * 
 * @author jameszhen
 *
 */
public class AzureDocumentLoader {

	private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
			.getLogger(AzureDocumentLoader.class);

	private static DocumentClient documentClient;
	private static DocumentCollection collection;
	private static Database db;

	public AzureDocumentLoader(String documentDBHostUrl,
			String documentDBAccessKey, String databaseName,
			String collectionName) throws Exception {
		LOG.info(String
				.format("Define a Azure Document loader to load data to host: %s at database: %s.",
						documentDBHostUrl, databaseName));
		initialize(documentDBHostUrl, documentDBAccessKey, databaseName,
				collectionName);
	}

	/**
	 * Simple implementation of creating a singleton of documentDb client.
	 * 
	 * @param collectionName
	 * @param databaseName
	 * @throws DaoException
	 */
	private void initialize(String documentDBHostUrl,
			String documentDBAccessKey, String databaseName,
			String collectionName) throws Exception {

		try {
			
			if (documentClient == null) {
				LOG.info(String
						.format("Start DocumentDB client initialization with host: %s.",
								documentDBHostUrl));
				documentClient = new DocumentClient(documentDBHostUrl,
						documentDBAccessKey, null, ConsistencyLevel.Session);
			}
			List<Database> databaseList = documentClient
					.queryDatabases(
							"SELECT * FROM root r WHERE r.id='" + databaseName
									+ "'", null).getQueryIterable().toList();
			if (databaseList.size() > 0) {
				db = databaseList.get(0);
			}
			
			List<DocumentCollection> collectionList = documentClient
					.queryCollections(
							db.getSelfLink(),
							"SELECT * FROM root r WHERE r.id='"
									+ collectionName + "'", null)
					.getQueryIterable().toList();

			System.out.println("collectionList.size(): "+ collectionList.size());
			
			if (collectionList.size() > 0) {
				collection = collectionList.get(0);

			}

		} catch (Exception ex) {
			LOG.info("Failed to connect to DocumentDB and retrive db and collection");
			LOG.info(ex.getMessage());
			ex.printStackTrace();
		}

	}

	/**
	 * Load a list of Azure documents to the defined collection at Azure
	 * DocumentDb
	 * 
	 * @param collectionName
	 * @param documents
	 */
	public void loadDocuments(String collectionName, List<Document> documents)
			throws DocumentClientException {

		LOG.info(String.format(
				"Start to load %d of documents to collection %s.",
				documents.size(), collectionName));
//		int i = 1;
		for (Document document : documents) {
			documentClient.createDocument(collection.getSelfLink(),
					new Document(document.toString()), new RequestOptions(),
					false);
//			System.out.println("Document Ingested: "+ i);
//			i++;
		}
		System.out.println("Document Ingested: ");

	}

	public void deleteOldDocuments(String collectionName, String effDate)
			throws DocumentClientException {

		String queryString = "SELECT * FROM c where c.ingestedDate != \""
				+ effDate + "\"";
		List<Document> results = documentClient
				.queryDocuments(collection.getSelfLink(), queryString, null)
				.getQueryIterable().toList();

		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			List<String> test = new ArrayList<String>();

			// To delete a document you need the “_self�? id of the document. We
			// get that and add to list
			for (Document doc : results) {
				test.add(doc.get("_self").toString());
			}
			// Now we iterate through the list of _self and delete the documents
			if (test.size() > 0) {
				LOG.info("Number of documents to be deleted : " + test.size());
				System.out.println("Number of documents to be deleted : "
						+ test.size());
			} else {
				LOG.info("No documents to be deleted");
				System.out.println("No documents to be deleted");
			}
			for (int j = 0; j < test.size(); j++) {
				documentClient.deleteDocument(test.get(j), null);
				System.out.println("Documents deleted : " + j);
			}
			System.out.println("Deleted old documents in documentDB."
					+ dateFormat.format(new Date()));
			LOG.info("Deleted old documents in documentDB."
					+ dateFormat.format(new Date()));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}


}
