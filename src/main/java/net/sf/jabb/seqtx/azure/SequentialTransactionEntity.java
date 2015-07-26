/**
 * 
 */
package net.sf.jabb.txprogress.azure;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import net.sf.jabb.txprogress.ProgressTransactionState;

import com.microsoft.azure.storage.table.Ignore;
import com.microsoft.azure.storage.table.StoreAs;
import com.microsoft.azure.storage.table.TableServiceEntity;

/**
 * The entity stored in Azure table storage to represent a transaction.
 * <br>PartitionKey: progressId
 * <br>RowKey: transactionId
 * @author James Hu
 *
 */
public class ProgressTransactionEntity extends TableServiceEntity {
	static public final int MAX_BINARY_LENGTH = 64*1024;
	
	protected String processorId;
	protected String startPosition;
	protected String endPosition;
	protected Date timeout;
	protected Date startTime;
	protected Date finishTime;
	protected String state;
	protected byte[] serializedDetail;
	protected int attempts;
	
	protected String previousTransactionId;
	protected String nextTransactionId;
	
	@Ignore
	public void setTimeout(Instant timeout){
		this.timeout = Date.from(timeout);
	}
	@Ignore
	public Instant getTimeout(){
		return this.timeout.toInstant();
	}
	@Ignore
	public void setStartTime(Instant startTime){
		this.startTime = Date.from(startTime);
	}
	@Ignore
	public Instant getStartTime(){
		return this.startTime.toInstant();
	}
	@Ignore
	public void setFinishTime(Instant finishTime){
		this.finishTime = Date.from(finishTime);
	}
	@Ignore
	public Instant getFinishTime(){
		return this.finishTime.toInstant();
	}
	@Ignore
	public void setState(ProgressTransactionState state){
		this.state = state.name();
	}
	@Ignore
	public ProgressTransactionState getState(){
		return ProgressTransactionState.valueOf(this.state);
	}
	@Ignore
	public void setDetail(Serializable detail){
		this.serializedDetail = SerializationUtils.serialize(detail);
		Validate.isTrue(this.serializedDetail.length <= MAX_BINARY_LENGTH, 
				"Serialized transaction detail must not exceed %d bytes, that's the limitation of Azure table storage."
				, MAX_BINARY_LENGTH);
	}
	@Ignore
	public Serializable getDetail(){
		return (Serializable) SerializationUtils.deserialize(this.serializedDetail);
	}
	

	@Ignore
	public String getProgressId() {
		return this.getPartitionKey();
	}
	@Ignore
	public void setProgressId(String progressId) {
		this.setPartitionKey(progressId);
	}
	@Ignore
	public String getTransactionId() {
		return this.getRowKey();
	}
	@Ignore
	public void setTransactionId(String transactionId) {
		this.setRowKey(transactionId);
	}
	
	public String getProcessorId() {
		return processorId;
	}
	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}
	public String getStartPosition() {
		return startPosition;
	}
	public void setStartPosition(String startPosition) {
		this.startPosition = startPosition;
	}
	public String getEndPosition() {
		return endPosition;
	}
	public void setEndPosition(String endPosition) {
		this.endPosition = endPosition;
	}
	@StoreAs(name="timeout")
	public Date getTimeoutAsDate() {
		return timeout;
	}
	public void setTimeout(Date timeout) {
		this.timeout = timeout;
	}
	@StoreAs(name="startTime")
	public Date getStartTimeAsDate() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	@StoreAs(name="finishTime")
	public Date getFinishTimeAsDate() {
		return finishTime;
	}
	public void setFinishTime(Date finishTime) {
		this.finishTime = finishTime;
	}
	@StoreAs(name="state")
	public String getStateAsString() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	@StoreAs(name = "detail")
	public byte[] getSerializedDetail() {
		return serializedDetail;
	}
	@StoreAs(name = "detail")
	public void setSerializedDetail(byte[] serializedDetail) {
		this.serializedDetail = serializedDetail;
	}
	public int getAttempts() {
		return attempts;
	}
	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}
	@StoreAs(name = "previous")
	public String getPreviousTransactionId() {
		return previousTransactionId;
	}
	@StoreAs(name = "previous")
	public void setPreviousTransactionId(String previousTransactionId) {
		this.previousTransactionId = previousTransactionId;
	}
	@StoreAs(name = "next")
	public String getNextTransactionId() {
		return nextTransactionId;
	}
	@StoreAs(name = "next")
	public void setNextTransactionId(String nextTransactionId) {
		this.nextTransactionId = nextTransactionId;
	}
	
	@Ignore
	public void setFirstTransaction(){
		this.previousTransactionId = "";
	}
	@Ignore
	public boolean isFirstTransaction(){
		return previousTransactionId != null && previousTransactionId.length() == 0;
	}
	@Ignore
	public void setLastTransaction(){
		this.nextTransactionId = "";
	}
	@Ignore
	public boolean isLastTransaction(){
		return nextTransactionId != null && nextTransactionId.length() == 0;
	}

}
