/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;

import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;

/**
 * @author James Hu
 *
 */
public class SequentialTransactionWrapper {
	static public final int MAX_BINARY_LENGTH = 64*1024;

	
	protected DynamicTableEntity entity;
	protected SimpleSequentialTransaction transaction;
	protected SequentialTransactionWrapper previous;
	protected SequentialTransactionWrapper next;
	
	protected String seriesId;
	protected String previousTransactionId;
	protected String nextTransactionId;

	
	public SequentialTransactionWrapper(){
		
	}
	
	/**
	 * Create a new instance with a DynamicTableEntity.
	 * The transaction field of the newly created instance will be null, but the seriesId, previousTransactionId, and nextTransactionId fields will be initialized.
	 * @param entity	the entity from Azure table storage
	 */
	public SequentialTransactionWrapper(DynamicTableEntity entity){
		this.entity = entity;
		
		this.seriesId = entity.getPartitionKey();
		
		EntityProperty p = null;
		
		p = entity.getProperties().get("previous");
		this.previousTransactionId = p == null ? null : p.getValueAsString();

		p = entity.getProperties().get("next");
		this.nextTransactionId = p == null ? null : p.getValueAsString();
	}
	
	/**
	 * Create a new instance with a SimpleSequentialTransaction.
 	 * The entity field of the newly created instance will be null, so as other fields except the transaction field.
	 * @param transaction	the SimpleSequentialTransaction object
	 */
	public SequentialTransactionWrapper(SimpleSequentialTransaction transaction){
		this.transaction = transaction;
	}
	
	public void updateFromEntity(){
		if (transaction == null){
			transaction = new SimpleSequentialTransaction();
		}
		
		EntityProperty p = null;
		
		p = entity.getProperties().get("attempts");
		transaction.setAttempts(p == null ? 0 : p.getValueAsInteger());
		
		p = entity.getProperties().get("detail");
		transaction.setDetail(p == null ? null : (Serializable)SerializationUtils.deserialize(p.getValueAsByteArray()));

		p = entity.getProperties().get("endPosition");
		transaction.setEndPosition(p == null ? null : p.getValueAsString());

		p = entity.getProperties().get("finishTime");
		transaction.setFinishTime(p == null ? null : p.getValueAsDate().toInstant());
		
		p = entity.getProperties().get("processorId");
		transaction.setProcessorId(p == null ? null : p.getValueAsString());
		
		p = entity.getProperties().get("startPosition");
		transaction.setStartPosition(p == null ? null : p.getValueAsString());
		
		p = entity.getProperties().get("startTime");
		transaction.setStartTime(p == null ? null : p.getValueAsDate().toInstant());
		
		p = entity.getProperties().get("state");
		transaction.setState(p == null ? null : SequentialTransactionState.valueOf(p.getValueAsString()));
		
		p = entity.getProperties().get("timeout");
		transaction.setTimeout(p == null ? null : p.getValueAsDate().toInstant());
		
		transaction.setTransactionId(entity.getRowKey());
		this.seriesId = entity.getPartitionKey();
		
		p = entity.getProperties().get("previous");
		this.previousTransactionId = p == null ? null : p.getValueAsString();

		p = entity.getProperties().get("next");
		this.nextTransactionId = p == null ? null : p.getValueAsString();

	}
	
	public void updateToEntity(){
		if (entity == null){
			entity = new DynamicTableEntity();
		}
		
		Instant i = null;
		SequentialTransactionState s = null;
		
		entity.getProperties().put("attempts", new EntityProperty(transaction.getAttempts()));
		
		if (transaction.getDetail() != null){
			byte[] serializedDetail = SerializationUtils.serialize(transaction.getDetail());
			Validate.isTrue(serializedDetail.length <= MAX_BINARY_LENGTH, 
					"Serialized transaction detail must not exceed %d bytes, that's the limitation of Azure table storage."
					, MAX_BINARY_LENGTH);
			entity.getProperties().put("detail", new EntityProperty(serializedDetail));
		}else{
			entity.getProperties().remove("detail");
		}
		
		entity.getProperties().put("endPosition", new EntityProperty(transaction.getEndPosition()));
		
		i = transaction.getFinishTime();
		entity.getProperties().put("finishTime", new EntityProperty(i == null ? null : Date.from(i)));
		
		
		entity.getProperties().put("processorId", new EntityProperty(transaction.getProcessorId()));
		
		entity.getProperties().put("startPosition", new EntityProperty(transaction.getStartPosition()));
		
		i = transaction.getStartTime();
		entity.getProperties().put("startTime", new EntityProperty(i == null ? null : Date.from(i)));
		
		s = transaction.getState();
		entity.getProperties().put("state", new EntityProperty(s == null ? null : s.name()));
		
		i = transaction.getTimeout();
		entity.getProperties().put("timeout", new EntityProperty(i == null ? null : Date.from(i)));
		
		entity.setRowKey(transaction.getTransactionId());
		
		entity.setPartitionKey(seriesId);
		entity.getProperties().put("previous", new EntityProperty(this.previousTransactionId));
		entity.getProperties().put("next", new EntityProperty(this.nextTransactionId));
	}
	
	public SimpleSequentialTransaction getTransactionNotNull(){
		if (transaction == null){
			updateFromEntity();
		}
		return transaction;
	}
	
	/**
	 * Mark this as the first transaction, also changes the entity field.
	 */
	public void setFirstTransaction(){
		this.previousTransactionId = "";
		if (entity != null){
			entity.getProperties().put("previous", new EntityProperty(""));
		}
	}
	public boolean isFirstTransaction(){
		return previousTransactionId == null || previousTransactionId.length() == 0;
	}
	
	/**
	 * Mark this as the last transaction, also changes the entity field.
	 */
	public void setLastTransaction(){
		this.nextTransactionId = "";
		if (entity != null){
			entity.getProperties().put("next", new EntityProperty(""));
		}
	}
	public boolean isLastTransaction(){
		return nextTransactionId == null || nextTransactionId.length() == 0;
	}

	public String entityKeysToString(){
		return entity == null ? null : (entity.getPartitionKey() + "/" + entity.getRowKey());
	}
	
	public String getEntityTransactionId(){
		return entity == null ? null : entity.getPartitionKey();
	}

	public DynamicTableEntity getEntity() {
		return entity;
	}
	public void setEntity(DynamicTableEntity entity) {
		this.entity = entity;
	}
	public SequentialTransactionWrapper getPrevious() {
		return previous;
	}
	public void setPrevious(SequentialTransactionWrapper previous) {
		this.previous = previous;
	}
	public SequentialTransactionWrapper getNext() {
		return next;
	}
	public void setNext(SequentialTransactionWrapper next) {
		this.next = next;
	}

	public SimpleSequentialTransaction getTransaction() {
		return transaction;
	}

	public void setTransaction(SimpleSequentialTransaction transaction) {
		this.transaction = transaction;
	}

	public String getSeriesId() {
		return seriesId;
	}

	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	public String getPreviousTransactionId() {
		return previousTransactionId;
	}

	public void setPreviousTransactionId(String previousTransactionId) {
		this.previousTransactionId = previousTransactionId;
	}

	public String getNextTransactionId() {
		return nextTransactionId;
	}

	public void setNextTransactionId(String nextTransactionId) {
		this.nextTransactionId = nextTransactionId;
	}
}
