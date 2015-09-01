/**
 * 
 */
package net.sf.jabb.azure;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
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
	 * the value is &gt;= prefix and &lt;= prefix + "\uFFFF"
	 * @param property		name of the property
	 * @param prefix		the prefix that the filter condition requires
	 * @return	the filter condition string
	 */
	public static String generateStartWithFilterCondition(String property, String prefix){
		return generateStartWithFilterCondition(property, prefix, "\uFFFF");
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
		return partitionKey + "/" + rowKey;
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
	static public boolean deleteIfExist(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException {
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
	static public boolean createIfNotExist(CloudTableClient tableClient, String tableName, AttemptStrategy attemptStrategy) throws URISyntaxException, StorageException {
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
	static public boolean createIfNotExist(CloudTableClient tableClient, String tableName) throws URISyntaxException, StorageException{
		return createIfNotExist(tableClient, tableName, DEFAULT_CREATION_ATTEMPT_STRATEGY);
	}
	
	/**
	 * Delete a queue if it exists.
	 * @param queueClient		instance of CloudQueueClient
	 * @param queueName			name of the queue
	 * @return						true if the queue existed in the storage service and has been deleted; otherwise false.
	 * @throws URISyntaxException	If the resource URI constructed based on the tableName is invalid.
	 * @throws StorageException		If a storage service error occurred during the operation.
	 */
	static public boolean deleteIfExist(CloudQueueClient queueClient, String queueName) throws URISyntaxException, StorageException {
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
	static public boolean createIfNotExist(CloudQueueClient queueClient, String queueName, AttemptStrategy attemptStrategy) throws URISyntaxException, StorageException {
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
	static public boolean createIfNotExist(CloudQueueClient queueClient, String queueName) throws URISyntaxException, StorageException{
		return createIfNotExist(queueClient, queueName, DEFAULT_CREATION_ATTEMPT_STRATEGY);
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
	 * @throws StorageException		if non-404 error happened
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
	 * @throws StorageException		if non-404 error happened
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
	 * @throws StorageException		if non-404 error happened
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
	 * @throws StorageException		if non-404 error happened
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
