/**
 * 
 */
package net.sf.jabb.taskq.azure;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.taskq.ReadOnlyScheduledTask;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import com.microsoft.azure.storage.table.Ignore;
import com.microsoft.azure.storage.table.StoreAs;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableServiceEntity;

/**
 * The task entity in table
 * <br>PartitionKey: queue + right(taskId, 2)
 * <br>RowKey: taskId
 * @author James Hu
 *
 */
public class TaskEntity extends TableServiceEntity implements ReadOnlyScheduledTask{
	static public final int MAX_BINARY_LENGTH = 64*1024;
	static private final String DELIMITER_IN_FULL_TASK_ID = "/";
	static private final String DELIMITER_IN_PARTITION_KEY = "^";

	protected String predecessorId;
	protected String processorId;
	protected Instant expectedExecutionTime;
	protected Instant visibleTime;
	protected Serializable detail;
	protected int attempts;
	
	static public String filterByVisibleTimeNoLaterThan(Instant time){
		return TableQuery.generateFilterCondition("V", QueryComparisons.LESS_THAN_OR_EQUAL, Date.from(time));
	}
	
	static public String filterByQueueName(String queueName){
		return AzureStorageUtility.generateStartWithFilterCondition(AzureStorageUtility.PARTITION_KEY, queueName + DELIMITER_IN_PARTITION_KEY);
	}

	public TaskEntity(){
		
	}

	public TaskEntity(String queueName, String taskId, int taskIdLengthInPartitionKey){
		AzureStorageUtility.validateCharactersInKey(queueName);
		AzureStorageUtility.validateCharactersInKey(taskId);
		Validate.isTrue(!queueName.contains(DELIMITER_IN_PARTITION_KEY), "Character sequence '{}' is not allowed in queue name: {}", DELIMITER_IN_PARTITION_KEY, queueName);
		Validate.isTrue(!taskId.contains(DELIMITER_IN_PARTITION_KEY), "Character sequence '{}' is not allowed in task ID: {}", DELIMITER_IN_PARTITION_KEY, taskId);
		this.rowKey = taskId;
		this.partitionKey = partitionKey(queueName, taskId, taskIdLengthInPartitionKey);
	}
	
	public TaskEntity(String queueName, String taskId, Serializable detail, Instant expectedExecutionTime, int taskIdLengthInPartitionKey){
		this(queueName, taskId, taskIdLengthInPartitionKey);
		this.detail = detail;
		this.expectedExecutionTime = expectedExecutionTime;
		this.visibleTime = expectedExecutionTime;
	}
	
	public TaskEntity(String queueName, String taskId, Serializable detail, Instant expectedExecutionTime, String predecessorId, int taskIdLengthInPartitionKey){
		this(queueName, taskId, detail, expectedExecutionTime, taskIdLengthInPartitionKey);
		this.predecessorId = predecessorId;
	}
	
	/**
	 * Create and return a copy of this object
	 * @return	the copy of this object
	 */
	public TaskEntity copy(){
		TaskEntity that = new TaskEntity();
		that.partitionKey = this.partitionKey;
		that.rowKey = this.rowKey;
		that.predecessorId = this.predecessorId;
		that.processorId = this.processorId;
		that.expectedExecutionTime = this.expectedExecutionTime;
		that.visibleTime = this.visibleTime;
		that.detail = this.detail;
		that.attempts = this.attempts;
		return that;
	}
	
	/**
	 * Determine the partition key from queue name and task id.
	 * @param queueName		name of the queue
	 * @param taskIdInQueue		id of the task in queue
	 * @param taskIdLengthInPartitionKey length of taskId rear part to be put into partition key
	 * @return		partition key of the entity representing the task
	 */
	static public String partitionKey(String queueName, String taskIdInQueue, int taskIdLengthInPartitionKey){
		return queueName + DELIMITER_IN_PARTITION_KEY + taskIdInQueue.substring(taskIdInQueue.length() - taskIdLengthInPartitionKey);
	}
	
	/**
	 * Determine the row key from queue name and task id
	 * @param queueName		name of the queue
	 * @param taskIdInQueue		id of the task in queue
	 * @return		row key of the entity representing the task
	 */
	static public String rowKey(String queueName, String taskIdInQueue){
		return taskIdInQueue;
	}
	
	/**
	 * Determine the partition and row keys from full task id
	 * @param fullTaskId	the full task id
	 * @param taskIdLengthInPartitionKey length of taskId rear part to be put into partition key
	 * @return	a 2-element array containing partition key and row key of the entity representing the task
	 */
	static public String[] partitionAndRowKeys(String fullTaskId, int taskIdLengthInPartitionKey){
		Validate.notNull(fullTaskId);
		int i = fullTaskId.indexOf(DELIMITER_IN_FULL_TASK_ID);
		Validate.isTrue(i >= 0, "Delimiter '{}' must exist in the full task ID: {}", DELIMITER_IN_FULL_TASK_ID, fullTaskId);
		String queueName = fullTaskId.substring(0, i);
		String taskId = fullTaskId.substring(i + DELIMITER_IN_FULL_TASK_ID.length());
		return new String[]{partitionKey(queueName, taskId, taskIdLengthInPartitionKey), rowKey(queueName, taskId)};
	}
	
	/**
	 * Get the full task ID which contains both the queue name and the task ID
	 * @return	the full task ID
	 */
	@Override
	@Ignore
	public String getTaskId(){
		return getQueueName() + DELIMITER_IN_FULL_TASK_ID + getTaskIdInQueue();
	}
	
	
	/**
	 * @return the processorId
	 */
	@StoreAs(name="P")
	public String getProcessorId() {
		return processorId;
	}

	/**
	 * @param processorId the processorId to set
	 */
	@StoreAs(name="P")
	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}

	/**
	 * @return the serialized detail
	 */
	@StoreAs(name = "D")
	public byte[] getSerializedDetail() {
		if (detail == null){
			return null;
		}else{
			byte[] serializedDetail = SerializationUtils.serialize(detail);
			Validate.isTrue(serializedDetail.length <= MAX_BINARY_LENGTH, 
					"Serialized task detail must not exceed %d bytes, that's the limitation of Azure table storage."
					, MAX_BINARY_LENGTH);
			return serializedDetail;
		}
	}

	/**
	 * @param serializedDetail the serialized detail
	 */
	@StoreAs(name = "D")
	public void setSerializedDetail(byte[] serializedDetail) {
		this.detail = (Serializable)SerializationUtils.deserialize(serializedDetail);
	}

	/**
	 * @param expectedExecutionTime the time this task is expected to be executed
	 */
	@StoreAs(name = "E")
	public void setExpectedExecutionTimeAsDate(Date expectedExecutionTime) {
		this.expectedExecutionTime = expectedExecutionTime == null ? null : expectedExecutionTime.toInstant();
	}
	
	@StoreAs(name = "E")
	public Date getExpectedExecutionTimeAsDate(){
		return this.expectedExecutionTime == null ? null : Date.from(this.expectedExecutionTime);
	}

	/**
	 * @param attempts the attempts to set
	 */
	@StoreAs(name = "A")
	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}

	@Override
	@StoreAs(name = "A")
	public int getAttempts() {
		return attempts;
	}

	@Override
	@Ignore
	public Instant getExpectedExecutionTime() {
		return expectedExecutionTime;
	}

	@Ignore
	public void setExpectedExecutionTime(Instant expectedExecutionTime) {
		this.expectedExecutionTime = expectedExecutionTime;
	}

	@Override
	@StoreAs(name="C")
	public String getPredecessorId() {
		return predecessorId;
	}

	@StoreAs(name="C")
	public void setPredecessorId(String predecessorId) {
		this.predecessorId = predecessorId;
	}

	@Ignore
	public void setDetail(Serializable detail){
		this.detail = detail;
	}
	
	@Override
	@Ignore
	public Serializable getDetail(){
		return this.detail;
	}
	
	@Ignore
	public String getTaskIdInQueue() {
		return this.rowKey;
	}
	
	@Ignore
	public String getQueueName(){
		return this.partitionKey.substring(0, partitionKey.indexOf(DELIMITER_IN_PARTITION_KEY));
	}
	
	/**
	 * @return the time that this task will be visible for execution
	 */
	@StoreAs(name="V")
	public Date getVisibleTimeAsDate() {
		return this.visibleTime == null ? null : Date.from(this.visibleTime);
	}

	/**
	 * @param visibleTimeAsDate the time that this task will be visible for execution
	 */
	@StoreAs(name="V")
	public void setVisibleTimeAsDate(Date visibleTimeAsDate) {
		this.visibleTime = visibleTimeAsDate == null ? null : visibleTimeAsDate.toInstant();
	}

	@Ignore
	public Instant getVisibleTime() {
		return visibleTime;
	}

	@Ignore
	public void setVisibleTime(Instant visibleTime) {
		this.visibleTime = visibleTime;
	}

}
