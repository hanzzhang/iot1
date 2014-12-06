package com.packtpub.storm.trident.azure;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.BlockSearchMode;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.core.Base64;

public class BlobWriter {
	static public void upload(Properties properties, String blobname, String blockIdStr, String data) {
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		InputStream stream = null;
		try {
			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");

			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			logger.info("accountName = " + accountName);
			logger.info("accountKey = " + accountKey);
			logger.info("containerName = " + containerName);
			logger.info("connectionStr = " + connectionStr);

			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();
			ArrayList<BlockEntry> newBlockList = new ArrayList<BlockEntry>();

			stream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
			BlockEntry newBlock = new BlockEntry(Base64.encode(blockIdStr.getBytes()), BlockSearchMode.UNCOMMITTED);
			blockBlob.uploadBlock(newBlock.getId(), stream, -1);
			newBlockList.add(newBlock);

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			blocksBeforeUpload.addAll(newBlockList);
			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	static public void remove(Properties properties, String blockIdStrFormat, String blobname, String blockIdStr) {
		// remove blocks with blockid >= blockIdStr
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		try {
			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");

			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			logger.info("accountName = " + accountName);
			logger.info("accountKey = " + accountKey);
			logger.info("containerName = " + containerName);
			logger.info("connectionStr = " + connectionStr);

			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			int blockid = Integer.parseInt(blockIdStr);
			int size = blocksBeforeUpload.size();
			//int size = 50000;
			for (int i = size; i >= blockid; i--) {
				String idStr = String.format(blockIdStrFormat, i);
				BlockEntry entry = new BlockEntry(Base64.encode(idStr.getBytes()), BlockSearchMode.UNCOMMITTED);
				if (blocksBeforeUpload.contains(entry)) {
					blocksBeforeUpload.remove(entry);
				}
			}
			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}