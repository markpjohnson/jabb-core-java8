/**
 * 
 */
package net.sf.jabb.azure;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableEntity;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

/**
 * Utility functions for Azure Storage usage.
 * @author James Hu
 *
 */
public class AzureStorageUtility {
	static private final Logger logger = LoggerFactory.getLogger(AzureStorageUtility.class);
	
	static public final int DEFAULT_MAX_ATTEMPTS_FOR_CREATE_IF_NOT_EXIST = 100;
	static public final int DEFAULT_BASE_RETRY_INTERVAL_FOR_CREATE_IF_NOT_EXIST = 5;
	
    static public final String PARTITION_KEY = "PartitionKey";
    static public final String ROW_KEY = "RowKey";
    static public final String TIMESTAMP = "Timestamp";
    
	static public String[] COLUMNS_WITH_ONLY_KEYS = new String[0];


	
	static long calculateRetryInterval(int attempts, int baseRetryInterval){
		return (long) (Math.log1p(attempts)/Math.log(2) * 1000L * baseRetryInterval);
	}
	
	/**
	 * Delete a table if it exists.
	 * @param tableClient		instance of CloudTableClient
	 * @return						true if the table existed in the storage service and has been deleted; otherwise false.
	 * @param tableName			name of the table
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean deleteIfExist(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException {
			CloudTable table = tableClient.getTableReference(tableName);
			return table.deleteIfExists();
	}

	/**
	 * Create a table if it does not exist.
	 * @param tableClient		instance of CloudTableClient
	 * @param tableName			name of the table
	 * @param maxAttempts		Maximum number of attempts in the case that Azure returns 409 conflict due to table is being deleted
	 * @param baseRetryInterval	Base interval between retries in the case that Azure returns 409 conflict.
	 * 							The interval after attempt n is (Math.log1p(n)/Math.log(2) * 1000L * baseRetryInterval) milliseconds.
	 * @return						true if the table did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean createIfNotExist(CloudTableClient tableClient, String tableName, int maxAttempts, int baseRetryInterval) throws URISyntaxException, StorageException {
		CloudTable table = tableClient.getTableReference(tableName);
		int attempts = 1;
		do {
			try {
				return table.createIfNotExists();
			} catch(StorageException e) {
				if(e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.TABLE_BEING_DELETED.equals(e.getErrorCode())) {	// conflict - so that we need to retry
					try {
						Thread.sleep(calculateRetryInterval(attempts, baseRetryInterval));
					} catch (InterruptedException e1) {
						logger.error("Interrupted after attempted {} times to create table '{}'.", attempts, tableName);
						Thread.currentThread().interrupt();
					}
				} else {
					throw e;
				}
			}
		} while(attempts ++ <= maxAttempts);
		logger.error("Unable to create table '{}' after attempted {} times.", tableName, attempts - 1);
		return false;
	}
	
	/**
	 * Create a table if it does not exist. If the table is being deleted, the creation will be retried.
	 * The maximum number of attempts is 100. The retry interval starts at 5 seconds and increases to 33 seconds guardedly.
	 * So the total maximum duration may be as long as 45 minutes.
	 * @param tableClient		instance of CloudTableClient
	 * @param tableName			name of the table
	 * @return						true if the table did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean createIfNotExist(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException{
		return createIfNotExist(tableClient, tableName, DEFAULT_MAX_ATTEMPTS_FOR_CREATE_IF_NOT_EXIST, DEFAULT_BASE_RETRY_INTERVAL_FOR_CREATE_IF_NOT_EXIST);
	}
	
	/**
	 * Delete a queue if it exists.
	 * @param queueName			name of the queue
	 * @param queueClient		instance of CloudQueueClient
	 * @return						true if the queue existed in the storage service and has been deleted; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean deleteIfExist(String queueName, CloudQueueClient queueClient) throws URISyntaxException, StorageException {
		CloudQueue queue = queueClient.getQueueReference(queueName);
		return queue.deleteIfExists();
	}

	/**
	 * Create a queue if it does not exist.
	 * @param queueClient				instance of CloudQueueClient
	 * @param queueName					name of the queue
	 * @param maxAttempts				Maximum number of attempts in the case that Azure returns 409 conflict due to table is being deleted
	 * @param baseRetryInterval	Base interval between retries in the case that Azure returns 409 conflict. 
	 * 							The interval after attempt n is (Math.log1p(n)/Math.log(2) * 1000L * baseRetryInterval) milliseconds.
	 * @return						true if the queue did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean createIfNotExist(CloudQueueClient queueClient, String queueName, int maxAttempts, int baseRetryInterval) throws URISyntaxException, StorageException {
		CloudQueue queue = queueClient.getQueueReference(queueName);
		int attempts = 1;
		do {
			try {
				return queue.createIfNotExists();
			} catch(StorageException e) {
				if(e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.QUEUE_BEING_DELETED.equals(e.getErrorCode())) {	// conflict - so that we need to retry
					try {
						Thread.sleep(calculateRetryInterval(attempts, baseRetryInterval));
					} catch (InterruptedException e1) {
						logger.error("Interrupted after attempted {} times to create queue '{}'.", attempts, queueName);
						Thread.currentThread().interrupt();
					}
				} else {
					throw e;
				}
			}
		} while(attempts ++ <= maxAttempts);
		logger.error("Unable to create queue '{}' after attempted {} times.", queueName, attempts - 1);
		return false;
	}
	
	/**
	 * Create a queue if it does not exist. If the queue is being deleted, the creation will be retried.
	 * The maximum number of attempts is 100. The retry interval starts at 5 seconds and increases to 33 seconds guardedly.
	 * So the total maximum duration may be as long as 45 minutes.
	 * @param queueClient		instance of CloudQueueClient
	 * @param queueName			name of the queue
	 * @return						true if the queue did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean createIfNotExist(CloudQueueClient queueClient, String queueName) throws URISyntaxException, StorageException{
		return createIfNotExist(queueClient, queueName, DEFAULT_MAX_ATTEMPTS_FOR_CREATE_IF_NOT_EXIST, DEFAULT_BASE_RETRY_INTERVAL_FOR_CREATE_IF_NOT_EXIST);
	}
	
	/**
	 * Execute an operation and ignore 404 not found error.
	 * @param table			the table
	 * @param operation		the operation
	 * @return				true if the operation succeeded, false if 404 not found error happened. 
	 * 						Please note that due to the retry logic inside Azure Storage SDK, 
	 * 						even if this method returns false it may be caused by a retry rather than the first attempt. 
	 * @throws StorageException
	 */
	static public boolean executeIfExist(CloudTable table, TableOperation operation) throws StorageException{
		try {
			table.execute(operation);
			return true;
		} catch (StorageException e) {
			if (e.getHttpStatusCode() == 404){ 
				return false;
			}else{
				throw e;
			}
		}
	}
	
	/**
	 * Execute an operation and ignore 404 not found error.
	 * @param table			the table
	 * @param operation		the operation
	 * @return				true if the operation succeeded, false if 404 not found error happened. 
	 * 						Please note that due to the retry logic inside Azure Storage SDK, 
	 * 						even if this method returns false it may be caused by a retry rather than the first attempt. 
	 * @throws StorageException
	 */
	static public boolean executeIfExist(CloudTable table, TableBatchOperation operation) throws StorageException{
		try {
			table.execute(operation);
			return true;
		} catch (StorageException e) {
			if (e.getHttpStatusCode() == 404){ 
				return false;
			}else{
				throw e;
			}
		}
	}

	
	/**
	 * Delete entities specified by a filtering condition. 404 not found error will be ignored.
	 * @param table		the table
	 * @param filter		the filter specifies the entities to be deleted
	 * @throws StorageException
	 */
	static public void deleteEntitiesIfExist(CloudTable table, String filter) throws StorageException{
		TableQuery<DynamicTableEntity> query = TableQuery.from(DynamicTableEntity.class)
				.select(COLUMNS_WITH_ONLY_KEYS);
		if (StringUtils.isNotBlank(filter)){
			query.where(filter);
		}
		for (DynamicTableEntity entity: table.execute(query)){
			TableOperation deleteOp = TableOperation.delete(entity);
			executeIfExist(table, deleteOp);
		}
	}

	/**
	 * Delete one entity specified by partition key and row key. 404 not found error will be ignored.
	 * @param table				the table
	 * @param partitionKey		the partition key of the entity
	 * @param rowKey			the row key of the entity
	 * @throws StorageException
	 */
	static public void deleteEntitiesIfExist(CloudTable table, String partitionKey, String rowKey) throws StorageException{
		String partitionFilter = TableQuery.generateFilterCondition(
				PARTITION_KEY, 
				QueryComparisons.EQUAL,
				partitionKey);
		String rowFilter = TableQuery.generateFilterCondition(
				ROW_KEY, 
				QueryComparisons.EQUAL,
				rowKey);
		String filter = TableQuery.combineFilters(partitionFilter, Operators.AND, rowFilter);
		deleteEntitiesIfExist(table, filter);
	}

	/**
	 * Delete one entity. 404 not found error will be ignored.
	 * @param table			the table
	 * @param entity		the entity to be deleted
	 * @throws StorageException
	 */
	static public void deleteEntitiesIfExist(CloudTable table, TableEntity entity) throws StorageException{
		String partitionKey = entity.getPartitionKey();
		String rowKey = entity.getRowKey();
		deleteEntitiesIfExist(table, partitionKey, rowKey);
	}


	/**
	 * List the blobs inside a container, excluding virtual directories.
	 * @param blobClient		instance of CloudBlobClient
	 * @param containerName		Name of the container
	 * @param prefix			Blob name prefix, can be null. Please note that if useFlatBlobListing is true, then the blob name actually contains the virtual directory path.
	 * @param regexNamePattern	Blob name pattern, can be null
	 * @param useFlatBlobListing <code>true</code> to indicate that the returned list will be flat; <code>false</code> to indicate that
     *            the returned list will be hierarchical.
	 * @param comparator		Comparator used for sorting, can be null
	 * @return		List of the blob items, filtered and sorted according to the arguments passed in
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public List<CloudBlob> listBlobs(CloudBlobClient blobClient, String containerName, String prefix, String regexNamePattern, 
			boolean useFlatBlobListing, Comparator<? super CloudBlob> comparator) throws URISyntaxException, StorageException{
		List<CloudBlob> result = new LinkedList<>();
		Pattern pattern = regexNamePattern == null ? null : Pattern.compile(regexNamePattern);

		CloudBlobContainer container = blobClient.getContainerReference(containerName);
		for(ListBlobItem blobItem : container.listBlobs(prefix, useFlatBlobListing, EnumSet.noneOf(BlobListingDetails.class), null, null)) {
			// If the item is a blob, not a virtual directory.
			if(blobItem instanceof CloudBlob) {
				CloudBlob blob = (CloudBlob) blobItem;
				String name = blob.getName();
				if(pattern == null || pattern.matcher(name).matches()) {
					result.add(blob);
				}
			}
		}

		if (comparator != null){
			Collections.sort(result, comparator);
		}
		return result;
	}
	
	
	static public CloudBlockBlob getBlockBlob(CloudBlobClient blobClient, String containerName, String blobName) throws URISyntaxException, StorageException{
		CloudBlobContainer container = blobClient.getContainerReference(containerName);
		CloudBlockBlob blob = container.getBlockBlobReference(blobName);
		return blob;
	}
	
	static public CloudBlockBlob getBlockBlob(CloudBlobClient blobClient, String relativePath) throws URISyntaxException, StorageException{
		String delimiter = "/";
		int i = relativePath.indexOf(delimiter);
		String containerName = relativePath.substring(0, i);
		String blobName = relativePath.substring(i + delimiter.length());
		return getBlockBlob(blobClient, containerName, blobName);
	}
	
	static public String getRelativePath(CloudBlob blob) throws StorageException, URISyntaxException{
		String delimiter = "/";
		CloudBlobContainer container = blob.getContainer();
		String containerName = container.getName();	// Container names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
		return containerName + delimiter + blob.getName();
	}
	

}
