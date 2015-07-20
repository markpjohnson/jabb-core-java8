/**
 * 
 */
package net.sf.jabb.txprogress;

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
	protected int attempts;
	
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
	
	/**
	 * Create a value copy of another ProgressTransaction
	 * @param that	an instance of ProgressTransaction
	 * @return	a newly created BasicProgressTransaction with the same field values as the argument 
	 */
	public static BasicProgressTransaction copyOf(ProgressTransaction that){
		BasicProgressTransaction copy = new BasicProgressTransaction();
		copy.transactionId = that.getTransactionId();
		copy.processorId = that.getProcessorId();
		copy.startPosition = that.getStartPosition();
		copy.endPosition = that.getEndPosition();
		copy.timeout = that.getTimeout();
		copy.transaction = that.getTransaction();
		copy.state = that.getState();
		copy.startTime = that.getStartTime();
		copy.finishTime = that.getFinishTime();
		return copy;
	}
	
	public boolean finish(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.finish()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
			return true;
		}
		return false;
	}
	
	public boolean abort(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.abort()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
			return true;
		}
		return false;
	}
	
	public boolean retry(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.retry()){
			this.startTime = Instant.now();
			this.finishTime = null;
			this.state = stateMachine.getState();
			return true;
		}
		return false;
	}
	
	public boolean timeout(){
		ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(this.state);
		if (stateMachine.timeout()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
			return true;
		}
		return false;
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
	@Override
	public int getAttempts() {
		return attempts;
	}
	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}
}
