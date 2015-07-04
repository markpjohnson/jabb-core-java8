/**
 * 
 */
package net.sf.jabb.transprogtracker;

import java.io.Serializable;
import java.time.Instant;

/**
 * A basic implementation of ProgressTransaction
 * @author James Hu
 *
 */
public class BasicProgressTransaction implements ProgressTransaction, Serializable{
	private static final long serialVersionUID = 498286656597820653L;

	protected String transactionId;
	protected String processorId;
	protected String startPosition;
	protected String endPosition;
	protected Instant timeout;
	protected Instant startTime;
	protected Instant finishTime;
	protected ProgressTransactionState state;
	protected Serializable transaction;
	
	public BasicProgressTransaction(){
		this(null, null, null, null, null, null);
	}
	
	/**
	 * Constructor to create a new in-progress transaction
	 * @param transactionId		ID of the transaction
	 * @param startPosition		the start position in the progress
	 * @param endPosition		the end position in the progress
	 * @param timeout			the time out time of this transaction
	 * @param startTime			start time of this transaction
	 * @param transaction		details of this transaction
	 */
	public BasicProgressTransaction(String transactionId, String processorId, String startPosition, String endPosition, Instant timeout, Serializable transaction){
		this(transactionId, processorId, startPosition, endPosition, timeout, Instant.now(), transaction);
	}
	
	/**
	 * Constructor to create a new in-progress transaction
	 * @param transactionId		ID of the transaction
	 * @param startPosition		the start position in the progress
	 * @param endPosition		the end position in the progress
	 * @param timeout			the time out time of this transaction
	 * @param startTime			start time of this transaction
	 * @param transaction		details of this transaction
	 */
	public BasicProgressTransaction(String transactionId, String processorId, String startPosition, String endPosition, Instant timeout, Instant startTime, Serializable transaction){
		this.transactionId = transactionId;
		this.processorId = processorId;
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.timeout = timeout;
		this.startTime = startTime;
		this.transaction = transaction;
		this.state = ProgressTransactionState.IN_PROGRESS;
	}
	
	public void finish(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.finish()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
		}else{
			throw new IllegalStateException("Can't finish in the state: " + this.state);
		}
	}
	
	public void abort(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.abort()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
		}else{
			throw new IllegalStateException("Can't abort in the state: " + this.state);
		}
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
	@Override
	public Instant getTimeout() {
		return timeout;
	}
	public void setTimeout(Instant timeout) {
		this.timeout = timeout;
	}
	@Override
	public Instant getStartTime() {
		return startTime;
	}
	public void setStartTime(Instant startTime) {
		this.startTime = startTime;
	}
	@Override
	public Instant getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(Instant finishTime) {
		this.finishTime = finishTime;
	}
	@Override
	public ProgressTransactionState getState() {
		return state;
	}
	public void setState(ProgressTransactionState state) {
		this.state = state;
	}
	@Override
	public Serializable getTransaction() {
		return transaction;
	}
	public void setTransaction(Serializable transaction) {
		this.transaction = transaction;
	}
	@Override
	public String getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	@Override
	public String getProcessorId() {
		return processorId;
	}
	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}
}
