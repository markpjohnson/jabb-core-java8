/**
 * 
 */
package net.sf.jabb.azure;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
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
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableServiceEntity;

/**
 * Utility functions for Azure Storage usage.
 * @author James Hu
 *
 */
public class AzureStorageUtility {
	static private final Logger logger = LoggerFactory.getLogger(AzureStorageUtility.class);
	
	/**
	 * The default attempt strategy for table/queue creation, with maximum 45 minutes allowed in total, and 1 to 40 seconds backoff interval 
	 * according to fibonacci series.
	 */
	static public final AttemptStrategy DEFAULT_CREATION_ATTEMPT_STRATEGY = new AttemptStrategy()
		.withWaitStrategy(WaitStrategies.threadSleepStrategy())
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofMinutes(45)))
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(1000L, 1000L * 40));
	
    static public final String PARTITION_KEY = "PartitionKey";
    static public final String ROW_KEY = "RowKey";
    static public final String TIMESTAMP = "Timestamp";
    
	static public String[] COLUMNS_WITH_ONLY_KEYS = new String[0];
	
	static final int MAX_BATCH_OPERATION_SIZE = 100;
	static final int MAX_GROUP_FOR_BATCH_OPERATION_SIZE = 1000;



	public static boolean isNotFoundOrUpdateConditionNotSatisfied(StorageException e){
		return e.getHttpStatusCode() == 404 || e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode());
	}
	
	public static boolean isNotFound(StorageException e){
		return e.getHttpStatusCode() == 404;
	}
	
	public static boolean isUpdateConditionNotSatisfied(StorageException e){
		return e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode());
	}
	
	public static boolean isEntityAlreadyExists(StorageException e){
		return e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.ENTITY_ALREADY_EXISTS.equals(e.getErrorCode());
	}
	
	public static boolean isNotFoundOrUpdateConditionNotSatisfied(Exception ex){
		if (ex instanceof StorageException){
			StorageException e = (StorageException)ex;
			return e.getHttpStatusCode() == 404 || e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode());
		}
		return false;
	}
	
	public static boolean isUpdateConditionNotSatisfied(Exception ex){
		if (ex instanceof StorageException){
			StorageException e = (StorageException)ex;
			return e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode());
		}
		return false;
	}
	
	public static boolean isEntityAlreadyExists(Exception ex){
		if (ex instanceof StorageException){
			StorageException e = (StorageException)ex;
			return e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.ENTITY_ALREADY_EXISTS.equals(e.getErrorCode());
		}
		return false;
	}
	
	public static boolean isNotFound(Exception ex){
		if (ex instanceof StorageException){
			StorageException e = (StorageException)ex;
			return e.getHttpStatusCode() == 404;
		}
		return false;
	}
	
	public static String combineTableQueryFilters(String operator, String filter1, String filter2, String... filters){
		String combined = TableQuery.combineFilters(filter1, operator, filter2);
		for (String filter: filters){
			combined = TableQuery.combineFilters(combined, operator, filter);
		}
		return combined;
	}
	
	/**
	 * Generate a filter condition for string start with a prefix by checking if
	 * the value is &gt;= prefix and &lt;= prefix + suffix
	 * @param property		name of the property
	 * @param prefix		the prefix that the filter condition requires
	 * @param suffix		the maximal (inclusive) allowed characters after the prefix
	 * @return	the filter condition string
	 */
	public static String generateStartWithFilterCondition(String property, String prefix, String suffix){
		return TableQuery.combineFilters(
				TableQuery.generateFilterCondition(property, QueryComparisons.GREATER_THAN_OR_EQUAL, prefix),
				TableQuery.Operators.AND,
				TableQuery.generateFilterCondition(property, QueryComparisons.LESS_THAN_OR_EQUAL, prefix + suffix)
				);
	}
	
	/**
	 * Generate a filter condition for string start with a prefix by checking if
	 * the value is &gt;= prefix and &lt;= prefix + "\u10FFFD"
	 * @param property		name of the property
	 * @param prefix		the prefix that the filter condition requires
	 * @return	the filter condition string
	 */
	public static String generateStartWithFilterCondition(String property, String prefix){
		return generateStartWithFilterCondition(property, prefix, "\u10FFFD");
	}
	
	/**
	 * Validate that all characters in the specified string are allowed in Azure table storage key fields.
	 * @param key	the key string to be validated
	 * @throws NullPointerException if the key string is null
	 * @throws IllegalArgumentException if any of the characters in the key string is not valid
	 */
	static public void validateCharactersInKey(String key){
		Validate.notNull(key);
		for (int i = 0; i < key.length(); i ++){
			char c = key.charAt(i);
			Validate.isTrue(c != '/', "The forward slash (/) character is not allowed: {}", key);
			Validate.isTrue(c != '\\', "The backslash (/) character is not allowed: {}", key);
			Validate.isTrue(c != '#', "The number sign (#) character is not allowed: {}", key);
			Validate.isTrue(c != '?', "The question mark (?) character is not allowed: {}", key);
			Validate.isTrue(! (c <= '\u001F' || c >= '\u007F' && c <= '\u009F'), "Control characters from U+0000 to U+001F and from U+007F to U+009F are not allowed: {}", key);
		}
	}
	

	
	/**
	 * Create a concatenated string of partitionKey + "/" + rowKey
	 * @param partitionKey		the partition key
	 * @param rowKey			the row key
	 * @return					the concatenated string
	 */
	static public String keysToString(String partitionKey, String rowKey){
		return (partitionKey == null ? "null" : partitionKey) + "/" + rowKey;
	}

	/**
	 * Create a concatenated string of partitionKey + "/" + rowKey
	 * @param entity		the table entity
	 * @return					the concatenated string
	 */
	static public String keysToString(TableEntity entity){
		return entity == null ? null : entity.getPartitionKey() + "/" + entity.getRowKey();
	}

	
	/**
	 * Delete a table if it exists.
	 * @param tableClient		instance of CloudTableClient
	 * @return						true if the table existed in the storage service and has been deleted; otherwise false.
	 * @param tableName			name of the table
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean deleteIfExists(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException {
			CloudTable table = tableClient.getTableReference(tableName);
			return table.deleteIfExists();
	}

	/**
	 * Create a table if it does not exist. If the table is being deleted, the creation will be retried.
	 * @param tableClient		instance of CloudTableClient
	 * @param tableName			name of the table
	 * @param attemptStrategy		attempt strategy in the case that Azure returns 409 conflict due to table is being deleted
	 * @return						true if the table did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws StorageException		If a storage service error occurred during the operation. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 */
	static public boolean createIfNotExists(CloudTableClient tableClient, String tableName, AttemptStrategy attemptStrategy) throws URISyntaxException, StorageException {
		CloudTable table = tableClient.getTableReference(tableName);
		return ExceptionUncheckUtility.getThrowingUnchecked(()->{
			return new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, 
					e-> e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.TABLE_BEING_DELETED.equals(e.getErrorCode()))	// conflict - so that we need to retry
				.call(()-> table.createIfNotExists());
		});
	}
	
	/**
	 * Create a table if it does not exist. If the table is being deleted, 
	 * the creation will be retried with the default attempt strategy: {@link #DEFAULT_CREATION_ATTEMPT_STRATEGY}}.
	 * @param tableClient		instance of CloudTableClient
	 * @param tableName			name of the table
	 * @return						true if the table did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws StorageException		If a storage service error occurred during the operation. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 */
	static public boolean createIfNotExists(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException{
		return createIfNotExists(tableClient, tableName, DEFAULT_CREATION_ATTEMPT_STRATEGY);
	}
	
	/**
	 * Delete a queue if it exists.
	 * @param queueClient		instance of CloudQueueClient
	 * @param queueName			name of the queue
	 * @return						true if the queue existed in the storage service and has been deleted; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean deleteIfExists(CloudQueueClient queueClient, String queueName) throws URISyntaxException, StorageException {
		CloudQueue queue = queueClient.getQueueReference(queueName);
		return queue.deleteIfExists();
	}

	/**
	 * Create a queue if it does not exist. If the queue is being deleted, the creation will be retried.
	 * @param queueClient				instance of CloudQueueClient
	 * @param queueName					name of the queue
	 * @param attemptStrategy		attempt strategy in the case that Azure returns 409 conflict due to table is being deleted
	 * @return						true if the queue did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the queueName is invalid. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws StorageException		If a storage service error occurred during the operation. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 */
	static public boolean createIfNotExists(CloudQueueClient queueClient, String queueName, AttemptStrategy attemptStrategy) throws URISyntaxException, StorageException {
		CloudQueue queue = queueClient.getQueueReference(queueName);
		return ExceptionUncheckUtility.getThrowingUnchecked(()->{
			return new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, 
						e-> e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.QUEUE_BEING_DELETED.equals(e.getErrorCode()))	// conflict - so that we need to retry
				.callThrowingSuppressed(()-> queue.createIfNotExists());
		});
	}
	
	/**
	 * Create a queue if it does not exist. If the queue is being deleted, the creation will be retried.
	 * the creation will be retried with the default attempt strategy: {@link #DEFAULT_CREATION_ATTEMPT_STRATEGY}}.
	 * @param queueClient		instance of CloudQueueClient
	 * @param queueName			name of the queue
	 * @return						true if the queue did not exist in the storage service and has been created successfully; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the queueName is invalid. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws StorageException		If a storage service error occurred during the operation. Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 */
	static public boolean createIfNotExists(CloudQueueClient queueClient, String queueName) throws URISyntaxException, StorageException{
		return createIfNotExists(queueClient, queueName, DEFAULT_CREATION_ATTEMPT_STRATEGY);
	}
	
	/**
	 * Execute an operation and ignore 404 not found error.
	 * @param table			the table
	 * @param operation		the operation
	 * @return				true if the operation succeeded, false if 404 not found error happened. 
	 * 						Please note that due to the retry logic inside Azure Storage SDK, 
	 * 						even if this method returns false it may be caused by a retry rather than the first attempt. 
	 * @throws StorageException		if non-404 error happened
	 */
	static public boolean executeIfExists(CloudTable table, TableOperation operation) throws StorageException{
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
	 * Operations in the batch may be split and retried if any of them got a 404 not found error
	 * @param table			the table
	 * @param operation		the operation
	 * @return				true if all the operations in the batch succeeded, 
	 * 						false if for each of the operations either it succeeded or got a 404 not found error. 
	 * 						Please note that due to the retry logic inside Azure Storage SDK, 
	 * 						even if this method returns false it may be caused by a retry rather than the first attempt. 
	 * @throws StorageException		if non-404 error happened
	 */
	static public boolean executeIfExists(CloudTable table, TableBatchOperation operation) throws StorageException{
		if (operation.size() == 0){
			return true;
		}
		if (operation.size() == 1){
			return executeIfExists(table, operation.get(0));
		}
		try {
			table.execute(operation);
			return true;
		} catch (StorageException e) {
			if (e.getHttpStatusCode() == 404){
				TableBatchOperation shouldSuceedBatch = new TableBatchOperation();	// all operations before the first failed
				TableBatchOperation notSureBatch = new TableBatchOperation();		// all not tried
				
				StorageExtendedErrorInformation extendedInfo = e.getExtendedErrorInformation();
				HashMap<String, String[]> details;
				if (extendedInfo != null && (details = extendedInfo.getAdditionalDetails()) != null){
					int i;
					for (i = 0; i < operation.size(); i ++){
						if (!details.containsKey(String.valueOf(i))){
							shouldSuceedBatch.add(operation.get(i));
						}else{
							break;
						}
					}
					for (; i < operation.size(); i ++){
						if (details.containsKey(String.valueOf(i))){
							executeIfExists(table, operation.get(i));
						}else{
							notSureBatch.add(operation.get(i));
						}
					}
				}else{
					shouldSuceedBatch.add(operation.get(0));
					notSureBatch.addAll(operation.subList(1, operation.size() - 1));
				}
				executeIfExists(table, shouldSuceedBatch);
				executeIfExists(table, notSureBatch);
				return false;
			}else{
				throw e;
			}
		}
	}

	/**
	 * Delete all entities in a partition. 404 not found error will be ignored.
	 * Deletion operations will be grouped into batches whenever possible.
	 * 
	 * @param table		the table
	 * @param partitionKey		the partition key
	 * @throws StorageException		if non-404 error happened
	 */
	static public void deleteEntitiesInPartitionIfExistsInBatches(CloudTable table, String partitionKey) throws StorageException{
		String partitionFilter = TableQuery.generateFilterCondition(
				PARTITION_KEY, 
				QueryComparisons.EQUAL,
				partitionKey);
		deleteEntitiesIfExistsInBatches(table, partitionFilter);
	}
	
	/**
	 * Delete entities specified by a filtering condition. 404 not found error will be ignored.
	 * Deletion operations will be grouped into batches whenever possible.
	 * 
	 * @param table		the table
	 * @param filter		the filter specifies the entities to be deleted
	 * @throws StorageException		if non-404 error happened
	 */
	static public void deleteEntitiesIfExistsInBatches(CloudTable table, String filter) throws StorageException{
		TableQuery<TableServiceEntity> query = TableQuery.from(TableServiceEntity.class)
				.select(COLUMNS_WITH_ONLY_KEYS);
		if (StringUtils.isNotBlank(filter)){
			query.where(filter);
		}
		
		List<TableServiceEntity> batch = new ArrayList<>(MAX_GROUP_FOR_BATCH_OPERATION_SIZE);
		for (TableServiceEntity entity: table.execute(query)){
			batch.add(entity);
			if (batch.size() >= MAX_GROUP_FOR_BATCH_OPERATION_SIZE){
				deleteEntitiesIfExistsInBatches(table, batch);
				batch.clear();
			}
		}
		if (batch.size() == 1){
			executeIfExists(table, TableOperation.delete(batch.get(0)));
		}else if (batch.size() > 1){
			deleteEntitiesIfExistsInBatches(table, batch);
		}
	}
	
	/**
	 * Delete entities using batch operations whenever possible.
	 * No exception will be thrown if an entity to be deleted does not exist.
	 * 
	 * @param table		the table
	 * @param allEntities		entities to be deleted, they don't need to be all in the same partition
	 * @throws StorageException	if any exception happened when executing batch deletion
	 */
	static public void deleteEntitiesIfExistsInBatches(CloudTable table, Collection<? extends TableEntity> allEntities) throws StorageException{
		Map<String, List<TableEntity>> groupedByPartitionKey = allEntities.stream().collect(Collectors.groupingBy(TableEntity::getPartitionKey));
		for (List<TableEntity> entities: groupedByPartitionKey.values()){
			if (entities.size() > 1){
				deleteEntitiesInSamePartitionIfExistsInBatches(table, entities);
			}else{
				executeIfExists(table, TableOperation.delete(entities.get(0)));
			}
		}
	}
	
	/**
	 * Delete entities in same partition using batch operations.
	 * No exception will be thrown if an entity to be deleted does not exist.
	 * 
	 * @param table		the table
	 * @param allEntities		entities to be deleted, they must have the same partition key
	 * @throws StorageException	if any exception happened when executing batch deletion
	 */
	static public void deleteEntitiesInSamePartitionIfExistsInBatches(CloudTable table, List<? extends TableEntity> allEntities) throws StorageException{
		TableBatchOperation batchOperation = new TableBatchOperation();
		for (List<? extends TableEntity> entities: Lists.partition(allEntities, MAX_BATCH_OPERATION_SIZE)){
			for (TableEntity entity: entities){
				batchOperation.delete(entity);
			}
			executeIfExists(table, batchOperation);
			batchOperation.clear();
		}
	}
	
	/**
	 * Retrieve an entity by row key only
	 * @param <T> type of the entity class
	 * @param table	the table
	 * @param rowKey	the row key
	 * @param clazzType	the entity class
	 * @return	the first entity that has the row key, or null if not found
	 */
	static public <T extends TableEntity> T retrieveByRowKey(CloudTable table, String rowKey, Class<T> clazzType){
		TableQuery<T> query = TableQuery.from(clazzType).where(
				TableQuery.generateFilterCondition(ROW_KEY, QueryComparisons.EQUAL, rowKey));
		for (T entity: table.execute(query)){
			return entity;
		}
		return null;
	}

	/**
	 * Retrieve an entity by row key only
	 * @param table	the table
	 * @param rowKey	the row key
	 * @return	the first entity that has the row key, or null if not found
	 */
	static public DynamicTableEntity retrieveByRowKey(CloudTable table, String rowKey){
		return retrieveByRowKey(table, rowKey, DynamicTableEntity.class);
	}

	/**
	 * Delete one entity specified by partition key and row key. 404 not found error will be ignored.
	 * @param table				the table
	 * @param partitionKey		the partition key of the entity
	 * @param rowKey			the row key of the entity
	 * @throws StorageException		if non-404 error happened
	 */
	static public void deleteEntityIfExists(CloudTable table, String partitionKey, String rowKey) throws StorageException{
		TableEntity entity = new TableServiceEntity(partitionKey, rowKey);
		setEtagAny(entity);
		executeIfExists(table, TableOperation.delete(entity));
	}

	/**
	 * Delete one entity. 404 not found error will be ignored.
	 * @param table			the table
	 * @param entity		the entity to be deleted
	 * @throws StorageException		if non-404 error happened
	 */
	static public void deleteEntitiesIfExists(CloudTable table, TableEntity entity) throws StorageException{
		executeIfExists(table, TableOperation.delete(entity));
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
	
	/**
	 * Mask the account key in the connection string
	 * @param connString	the original connection string
	 * @return		the string having account key masked, or the original string if there is no account key
	 */
	static public String maskAccountKey(String connString){
		String KEY = "AccountKey=";
		int i = StringUtils.indexOfIgnoreCase(connString, KEY);
		if (i >= 0){
			int j = connString.indexOf(';', i);
			if (j < 0){
				j = connString.length();
			}
			return connString.substring(0, i + KEY.length()) + "*****" + connString.substring(j);
		}else{
			return connString;
		}
	}

	static public void setEtagAny(TableEntity entity){
		entity.setEtag("*");
	}
}
