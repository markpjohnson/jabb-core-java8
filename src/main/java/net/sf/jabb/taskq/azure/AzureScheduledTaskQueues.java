/**
 * 
 */
package net.sf.jabb.taskq.azure;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.taskq.ReadOnlyScheduledTask;
import net.sf.jabb.taskq.ScheduledTaskQueues;
import net.sf.jabb.taskq.ex.NoSuchTaskException;
import net.sf.jabb.taskq.ex.NotOwningTaskException;
import net.sf.jabb.taskq.ex.TaskQueueStorageInfrastructureException;
import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.BiConsumerThrowsExceptions;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableRequestOptions;

/**
 * Implementation of <code>ScheduledTaskQueues</code> using Azure table storage
 * @author James Hu
 *
 */
public class AzureScheduledTaskQueues implements ScheduledTaskQueues{
	static private final Logger logger = LoggerFactory.getLogger(AzureScheduledTaskQueues.class);
	
	/**
	 * The default attempt strategy for Azure operations, with maximum 90 seconds allowed in total, and 0.5 to 10 seconds backoff interval 
	 * according to fibonacci series.
	 */
	static public final AttemptStrategy DEFAULT_ATTEMPT_STRATEGY = new AttemptStrategy()
		.withWaitStrategy(WaitStrategies.threadSleepStrategy())
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(90)))
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(500L, 1000L * 10));
	
	public static final String DEFAULT_TABLE_NAME = "ScheduledTaskQueues";
	
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile boolean tableExists = false;
	
	protected AttemptStrategy attemptStrategy = DEFAULT_ATTEMPT_STRATEGY;
	
	protected int taskIdLengthInPartitionKey = 2;


	public AzureScheduledTaskQueues(){
		
	}
	
	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount, String tableName, Integer taskIdLengthInPartitionKey, AttemptStrategy attemptStrategy, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		if (taskIdLengthInPartitionKey != null){
			this.taskIdLengthInPartitionKey = taskIdLengthInPartitionKey;
		}
		if (attemptStrategy != null){
			this.attemptStrategy = attemptStrategy;
		}
		tableClient = storageAccount.createCloudTableClient();
		if (defaultOptionsConfigurer != null){
			defaultOptionsConfigurer.accept(tableClient.getDefaultRequestOptions());
		}
	}
	
	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount, String tableName, Integer taskIdLengthInPartitionKey, AttemptStrategy attemptStrategy){
		this(storageAccount, tableName, taskIdLengthInPartitionKey, attemptStrategy, null);
	}

	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount, String tableName, Integer taskIdLengthInPartitionKey){
		this(storageAccount, tableName, taskIdLengthInPartitionKey, null, null);
	}

	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount, String tableName){
		this(storageAccount, tableName, null, null, null);
	}

	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount, Integer taskIdLengthInPartitionKey, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this(storageAccount, null, null, null, defaultOptionsConfigurer);
	}

	public AzureScheduledTaskQueues(CloudStorageAccount storageAccount){
		this(storageAccount, null, null, null);
	}
	
	public AzureScheduledTaskQueues(CloudTableClient tableClient, String tableName, Integer taskIdLengthInPartitionKey, AttemptStrategy attemptStrategy){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		if (taskIdLengthInPartitionKey != null){
			this.taskIdLengthInPartitionKey = taskIdLengthInPartitionKey;
		}
		this.tableClient = tableClient;
		if (attemptStrategy != null){
			this.attemptStrategy = attemptStrategy;
		}
	}

	public AzureScheduledTaskQueues(CloudTableClient tableClient, AttemptStrategy attemptStrategy){
		this(tableClient, null, null, attemptStrategy);
	}

	public AzureScheduledTaskQueues(CloudTableClient tableClient){
		this(tableClient, null, null, null);
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
	}

	public void setTableClient(AttemptStrategy attemptStrategy) {
		this.attemptStrategy = attemptStrategy;
	}
	
	/**
	 * @param taskIdLengthInPartitionKey the taskIdLengthInPartitionKey to set
	 */
	public void setTaskIdLengthInPartitionKey(int taskIdLengthInPartitionKey) {
		this.taskIdLengthInPartitionKey = taskIdLengthInPartitionKey;
	}

	protected String newUniqueTaskId(){
		return UUID.randomUUID().toString();
	}
	
	

	@Override
	public String put(String queue, Serializable detail, Instant expectedExecutionTime, String predecessorId)
			throws TaskQueueStorageInfrastructureException {
		Validate.notNull(queue, "Queue name cannot be null");
		Validate.notNull(expectedExecutionTime, "expected execution time cannot be null");

		String taskIdInQueue = newUniqueTaskId();
		TaskEntity task = new TaskEntity(queue, taskIdInQueue, detail, expectedExecutionTime, predecessorId, taskIdLengthInPartitionKey);
		CloudTable table = null;
		table = getTableReference();
		try {
			table.execute(TableOperation.insert(task));
		} catch (StorageException e) {
			if (!AzureStorageUtility.isEntityAlreadyExists(e)){		// if it is 409 then the insertion actually succeeded
				throw new TaskQueueStorageInfrastructureException("Insersion of new entity was not successful", e);
			}
		}
		
		return task.getTaskId();
	}

	@Override
	public List<ReadOnlyScheduledTask> get(String queue, Instant expectedExecutionTime, int maxNumOfTasks, String processorId, Instant timeout)
			throws TaskQueueStorageInfrastructureException {
		Validate.notNull(queue, "Queue name cannot be null");
		Validate.notNull(expectedExecutionTime, "expected execution time cannot be null");
		Validate.isTrue(maxNumOfTasks > 0, "Maximum number of tasks must be greater than zero");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(timeout, "Timeout time cannot be null");
		
		Map<String, Boolean> predecessorExistenceCache = new HashMap<>();
		List<ReadOnlyScheduledTask> result = new ArrayList<>(maxNumOfTasks);
		CloudTable table = null;
		table = getTableReference();
		try {
			// 
			TableQuery<TaskEntity> query = TableQuery.from(TaskEntity.class).
					where(
							AzureStorageUtility.combineTableQueryFilters(TableQuery.Operators.AND,
									TaskEntity.filterByQueueName(queue),
									TaskEntity.filterByVisibleTimeNoLaterThan(expectedExecutionTime)
									)
						);
			for (TaskEntity task: table.execute(query)){
				boolean predecessorExists = true;
				String predecessorId = task.getPredecessorId();
				if (predecessorId == null){
					predecessorExists = false;
				}else if (predecessorExistenceCache.containsKey(predecessorId)){
					predecessorExists = predecessorExistenceCache.get(predecessorId);
				}else{
					String[] predecessorKeys = TaskEntity.partitionAndRowKeys(predecessorId, taskIdLengthInPartitionKey);
					DynamicTableEntity predecessor = table.execute(
							TableOperation.retrieve(predecessorKeys[0], predecessorKeys[1], DynamicTableEntity.class)
							).getResultAsType();
					predecessorExistenceCache.put(predecessorId, predecessor != null);
					predecessorExists = predecessor != null;
				}
				
				if (!predecessorExists){
					task.setAttempts(task.getAttempts() + 1);
					task.setProcessorId(processorId);
					task.setVisibleTime(timeout);
					try{
						table.execute(TableOperation.replace(task));
					}catch(StorageException e){
						if (AzureStorageUtility.isNotFoundOrUpdateConditionNotSatisfied(e)){
							// just skip this one
							continue;
						}else{
							throw e;
						}
					}
					result.add(task);
					if (result.size() >= maxNumOfTasks){
						break;
					}
				}
			}
		} catch (Exception e) {
			throw new TaskQueueStorageInfrastructureException("Query of task entities was not successful", e);
		}
		return result;
	}
	
	protected void update(String id, String processorId, BiConsumerThrowsExceptions<CloudTable, TaskEntity> operation) throws NotOwningTaskException, NoSuchTaskException, TaskQueueStorageInfrastructureException{
		Validate.notNull(id, "Task ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");

		CloudTable table = getTableReference();
		try {
			String[] keys = TaskEntity.partitionAndRowKeys(id, taskIdLengthInPartitionKey);
			new AttemptStrategy(attemptStrategy)
				.retryIfException(AzureStorageUtility::isNotFoundOrUpdateConditionNotSatisfied)
				.run(()->{
					TaskEntity task = table.execute(
							TableOperation.retrieve(keys[0], keys[1], TaskEntity.class)
							).getResultAsType();
					if (task == null){
						throw new NoSuchTaskException("No task with ID '" + id + "' can be found");
					}
					if (!StringUtils.equals(processorId, task.getProcessorId())
								|| task.getVisibleTime().isBefore(Instant.now())){
							throw new NotOwningTaskException("Task with ID '" + id + "' is not currently owned by processor with ID '" + processorId + "'");
						}
					operation.accept(table, task);  // may throw isNotFoundOrUpdateConditionNotSatisfied
				});
		}catch(NotOwningTaskException | NoSuchTaskException | TaskQueueStorageInfrastructureException e){
			throw e;
		}catch(Exception e){
			throw new TaskQueueStorageInfrastructureException("Updating of task entity specified by ID '" + id + "' by processor with ID '" + processorId + "' was not successful", e);
		}
	}

	@Override
	public void finish(String id, String processorId) throws NotOwningTaskException, NoSuchTaskException, TaskQueueStorageInfrastructureException {
		update(id, processorId, (table, task) -> {
			table.execute(TableOperation.delete(task));
		});
	}

	@Override
	public void abort(String id, String processorId) throws NotOwningTaskException, NoSuchTaskException, TaskQueueStorageInfrastructureException {
		update(id, processorId, (table, task) -> {
			task.setVisibleTime(Instant.now());
			task.setProcessorId(null);
			table.execute(TableOperation.replace(task));
		});
	}

	@Override
	public void renewTimeout(String id, String processorId, Instant newTimeout) throws NotOwningTaskException, NoSuchTaskException,
			TaskQueueStorageInfrastructureException {
		Validate.notNull(newTimeout, "New timeout time cannot be null");

		update(id, processorId, (table, task) -> {
			task.setVisibleTime(newTimeout);
			table.execute(TableOperation.replace(task));
		});
	}

	@Override
	public void clear(String queue) throws TaskQueueStorageInfrastructureException {
		Validate.notNull(queue, "Queue name cannot be null");
		// delete entities by seriesId
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExist(table, 
					TaskEntity.filterByQueueName(queue));
			logger.debug("Deleted all tasks in queue '{}' in table: {}", queue, table == null ? null : table.getName()); 
		}catch(Exception e){
			throw new TaskQueueStorageInfrastructureException("Failed to delete entities belonging to queue '" + queue + "' in table: " + tableName, e);
		}
	}

	@Override
	public void clearAll() throws TaskQueueStorageInfrastructureException {
		// delete all entities
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExist(table, (String)null);
			logger.debug("Deleted all tasks in all queues in table: {}", table == null ? null : table.getName()); 
		}catch(Exception e){
			throw new TaskQueueStorageInfrastructureException("Failed to delete all entities in table: " + tableName, e);
		}
	}
	
	protected CloudTable getTableReference() throws TaskQueueStorageInfrastructureException{
		CloudTable table;
		try {
			table = tableClient.getTableReference(tableName);
		} catch (Exception e) {
			throw new TaskQueueStorageInfrastructureException("Failed to get reference for table: '" + tableName + "'", e);
		}
		if (!tableExists){
			try {
				if (AzureStorageUtility.createIfNotExist(tableClient, tableName)){
					logger.debug("Created table: {}", tableName); 
				}
			} catch (Exception e) {
				throw new TaskQueueStorageInfrastructureException("Failed to ensure the existence of table: '" + tableName + "'", e);
			}
			tableExists = true;
		}
		return table;
	}
	

}
