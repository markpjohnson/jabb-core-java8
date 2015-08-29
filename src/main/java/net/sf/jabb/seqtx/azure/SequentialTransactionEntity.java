/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.SequentialTransactionStateMachine;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import com.microsoft.azure.storage.table.Ignore;
import com.microsoft.azure.storage.table.StoreAs;
import com.microsoft.azure.storage.table.TableServiceEntity;

/**
 * The entity stored in Azure table storage to represent a transaction.
 * <br>PartitionKey: seriesId
 * <br>RowKey: transactionId
 * @author James Hu
 *
 */
public class SequentialTransactionEntity extends TableServiceEntity implements ReadOnlySequentialTransaction{
	static public final int MAX_BINARY_LENGTH = 64*1024;
	
	protected String processorId;
	protected String startPosition;
	protected String endPosition;
	protected Instant timeout;
	protected Instant startTime;
	protected Instant finishTime;
	protected SequentialTransactionState state;
	protected Serializable detail;
	protected int attempts;
	
	protected String previousTransactionId;
	protected String nextTransactionId;
	
	public SequentialTransaction toSequentialTransaction(){
		SimpleSequentialTransaction t = new SimpleSequentialTransaction();
		t.setAttempts(attempts);
		t.setDetail(getDetail());
		t.setEndPosition(endPosition);
		t.setFinishTime(getFinishTime());
		t.setProcessorId(processorId);
		t.setStartPosition(startPosition);
		t.setStartTime(getStartTime());
		t.setState(getState());
		t.setTimeout(getTimeout());
		t.setTransactionId(getTransactionId());
		return t;
	}
	
	public static SequentialTransactionEntity fromSequentialTransaction(String seriesId, ReadOnlySequentialTransaction t, String previousTransactionId, String nextTransactionId){
		SequentialTransactionEntity tx = new SequentialTransactionEntity();
		tx.setPartitionKey(seriesId);
		tx.setRowKey(t.getTransactionId());
		tx.processorId = t.getProcessorId();
		tx.startPosition = t.getStartPosition();
		tx.endPosition = t.getEndPosition();
		tx.timeout = t.getTimeout();
		tx.startTime = t.getStartTime();
		tx.finishTime = t.getFinishTime();
		tx.state = t.getState();
		tx.setDetail(t.getDetail());
		tx.attempts = t.getAttempts();
		tx.previousTransactionId = previousTransactionId;
		tx.nextTransactionId = nextTransactionId;
		
		return tx;
	}
	
	@Override
	public boolean isInProgress(){
		return SequentialTransactionState.IN_PROGRESS.name().equals(state);
	}
	
	@Override
	public boolean isFinished(){
		return SequentialTransactionState.FINISHED.name().equals(state);
	}
	
	@Override
	public boolean isFailed(){
		return SequentialTransactionState.ABORTED.name().equals(state) || SequentialTransactionState.TIMED_OUT.name().equals(state);
	}
	
	@Override
	public boolean hasStarted(){
		return startTime != null;
	}

	public String keysToString(){
		return AzureStorageUtility.keysToString(partitionKey, rowKey);
	}

	public boolean finish(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(getState());
		if (stateMachine.finish()){
			this.finishTime = Instant.now();
			setState(stateMachine.getState());
			return true;
		}
		return false;
	}
	
	public boolean abort(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(getState());
		if (stateMachine.abort()){
			this.finishTime = Instant.now();
			setState(stateMachine.getState());
			return true;
		}
		return false;
	}
	
	public boolean retry(String processorId, Instant timeout){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(getState());
		if (stateMachine.retry()){
			this.startTime = Instant.now();
			this.finishTime = null;
			setState(stateMachine.getState());
			this.attempts ++;
			this.processorId = processorId;
			this.timeout = timeout;
			return true;
		}
		return false;
	}
	
	public boolean timeout(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(getState());
		if (stateMachine.timeout()){
			this.finishTime = Instant.now();
			setState(stateMachine.getState());
			return true;
		}
		return false;
	}
	
	
	@Ignore
	public void setTimeout(Instant timeout){
		this.timeout = timeout;
	}
	@Override
	@Ignore
	public Instant getTimeout(){
		return timeout;
	}
	@Ignore
	public void setStartTime(Instant startTime){
		this.startTime = startTime;
	}
	@Override
	@Ignore
	public Instant getStartTime(){
		return startTime;
	}
	@Ignore
	public void setFinishTime(Instant finishTime){
		this.finishTime = finishTime;
	}
	@Override
	@Ignore
	public Instant getFinishTime(){
		return finishTime;
	}
	@Ignore
	public void setState(SequentialTransactionState state){
		this.state = state;
	}
	@Override
	@Ignore
	public SequentialTransactionState getState(){
		return state;
	}
	@Ignore
	public void setDetail(Serializable detail){
		if (detail == null){
			this.detail = null;
		}else{
			Validate.isTrue(SerializationUtils.serialize(detail).length <= MAX_BINARY_LENGTH, 
					"Serialized transaction detail must not exceed %d bytes, that's the limitation of Azure table storage."
					, MAX_BINARY_LENGTH);
			this.detail = detail;
		}
	}
	@Override
	@Ignore
	public Serializable getDetail(){
		return this.detail;
	}
	

	@Ignore
	public String getseriesId() {
		return this.getPartitionKey();
	}
	@Ignore
	public void setSeriesId(String seriesId) {
		AzureStorageUtility.validateCharactersInKey(seriesId);
		this.setPartitionKey(seriesId);
	}
	@Override
	@Ignore
	public String getTransactionId() {
		return this.getRowKey();
	}
	@Ignore
	public void setTransactionId(String transactionId) {
		AzureStorageUtility.validateCharactersInKey(transactionId);
		this.setRowKey(transactionId);
	}
	
	@Override
	public String getProcessorId() {
		return processorId;
	}
	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}
	@Override
	public String getStartPosition() {
		return startPosition;
	}
	public void setStartPosition(String startPosition) {
		this.startPosition = startPosition;
	}
	@Override
	public String getEndPosition() {
		return endPosition;
	}
	public void setEndPosition(String endPosition) {
		this.endPosition = endPosition;
	}
	@StoreAs(name="Timeout")
	public Date getTimeoutAsDate() {
		return timeout == null ? null : Date.from(timeout);
	}
	@StoreAs(name="Timeout")
	public void setTimeoutAsDate(Date timeout) {
		this.timeout = timeout == null ? null : timeout.toInstant();
	}
	@Ignore
	public void setTimeout(Date timeout) {
		setTimeoutAsDate(timeout);
	}
	@StoreAs(name="StartTime")
	public Date getStartTimeAsDate() {
		return startTime == null ? null : Date.from(startTime);
	}
	@StoreAs(name="StartTime")
	public void setStartTimeAsDate(Date startTime) {
		this.startTime = startTime == null ? null : startTime.toInstant();
	}
	@Ignore
	public void setStartTime(Date startTime) {
		setStartTimeAsDate(startTime);
	}
	@StoreAs(name="FinishTime")
	public Date getFinishTimeAsDate() {
		return finishTime == null ? null : Date.from(finishTime);
	}
	@StoreAs(name="FinishTime")
	public void setFinishTimeAsDate(Date finishTime) {
		this.finishTime = finishTime == null ? null : finishTime.toInstant();
	}
	@Ignore
	public void setFinishTime(Date finishTime) {
		setFinishTimeAsDate(finishTime);
	}
	@StoreAs(name="State")
	public String getStateAsString() {
		return state == null ? null : state.name();
	}
	@StoreAs(name="State")
	public void setStateAsString(String state) {
		this.state = state == null ? null : SequentialTransactionState.valueOf(state);
	}
	@Ignore
	public void setState(String state) {
		setStateAsString(state);
	}
	@StoreAs(name = "Detail")
	public byte[] getSerializedDetail() {
		return detail == null ? null : SerializationUtils.serialize(detail);
	}
	@StoreAs(name = "Detail")
	public void setSerializedDetail(byte[] serializedDetail) {
		this.detail = serializedDetail == null ? null : (Serializable) SerializationUtils.deserialize(serializedDetail);
	}
	@Override
	public int getAttempts() {
		return attempts;
	}
	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}
	@StoreAs(name = "Previous")
	public String getPreviousTransactionId() {
		return previousTransactionId;
	}
	@StoreAs(name = "Previous")
	public void setPreviousTransactionId(String previousTransactionId) {
		if (previousTransactionId == null){
			setFirstTransaction();
		}else{
			this.previousTransactionId = previousTransactionId;
		}
	}
	@StoreAs(name = "Next")
	public String getNextTransactionId() {
		return nextTransactionId;
	}
	@StoreAs(name = "Next")
	public void setNextTransactionId(String nextTransactionId) {
		if (nextTransactionId == null){
			setLastTransaction();
		}else{
			this.nextTransactionId = nextTransactionId;
		}
	}
	
	@Ignore
	public void setFirstTransaction(){
		this.previousTransactionId = "";
	}
	@Ignore
	public boolean isFirstTransaction(){
		return previousTransactionId == null || previousTransactionId.length() == 0;
	}
	@Ignore
	public void setLastTransaction(){
		this.nextTransactionId = "";
	}
	@Ignore
	public boolean isLastTransaction(){
		return nextTransactionId == null || nextTransactionId.length() == 0;
	}
	
}
