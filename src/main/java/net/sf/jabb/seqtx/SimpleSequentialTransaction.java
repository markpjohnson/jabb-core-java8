/**
 * 
 */
package net.sf.jabb.seqtx;

import java.io.Serializable;
import java.time.Instant;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A basic implementation of SequentialTransaction
 * @author James Hu
 *
 */
public class SimpleSequentialTransaction implements SequentialTransaction, Serializable{
	private static final long serialVersionUID = 498286656597820653L;

	protected String transactionId;
	protected String processorId;
	protected String startPosition;
	protected String endPosition;
	protected Instant timeout;
	protected Instant startTime;
	protected Instant finishTime;
	protected SequentialTransactionState state;
	protected Serializable detail;
	protected int attempts;
	
	public SimpleSequentialTransaction(){
		this(null, null, null, null, null, null);
	}
	
	/**
	 * Constructor to create a new transaction skeleton
	 * @param transactionId		ID of the transaction
	 * @param processorId		ID of the processor
	 * @param startPosition		the start position in the progress
	 * @param timeout			the time out time of this transaction
	 */
	public SimpleSequentialTransaction(String transactionId, String processorId, String startPosition, Instant timeout){
		this(transactionId, processorId, startPosition, null, timeout, Instant.now(), null);
		this.startTime = null;
		this.state = null;
		this.attempts = 0;
	}

	
	/**
	 * Constructor to create a new in-progress transaction
	 * @param transactionId		ID of the transaction
	 * @param processorId		ID of the processor
	 * @param startPosition		the start position in the progress
	 * @param endPosition		the end position in the progress
	 * @param timeout			the time out time of this transaction
	 * @param startTime			start time of this transaction
	 * @param detail			detail of this transaction
	 */
	public SimpleSequentialTransaction(String transactionId, String processorId, String startPosition, String endPosition, Instant timeout, Serializable detail){
		this(transactionId, processorId, startPosition, endPosition, timeout, Instant.now(), detail);
	}
	
	/**
	 * Constructor to create a new in-progress transaction
	 * @param transactionId		ID of the transaction
	 * @param processorId		ID of the processor
	 * @param startPosition		the start position in the progress
	 * @param endPosition		the end position in the progress
	 * @param timeout			the time out time of this transaction
	 * @param startTime			start time of this transaction
	 * @param detail			detail of this transaction
	 */
	public SimpleSequentialTransaction(String transactionId, String processorId, String startPosition, String endPosition, Instant timeout, Instant startTime, Serializable detail){
		this.transactionId = transactionId;
		this.processorId = processorId;
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.timeout = timeout;
		this.startTime = startTime;
		this.detail = detail;
		this.state = SequentialTransactionState.IN_PROGRESS;
		this.attempts = 1;
	}
	
	/**
	 * Create a value copy of another SequentialTransaction
	 * @param that	an instance of SequentialTransaction
	 * @return	a newly created SimpleSequentialTransaction with the same field values as the argument 
	 */
	public static SimpleSequentialTransaction copyOf(ReadOnlySequentialTransaction that){
		SimpleSequentialTransaction copy = new SimpleSequentialTransaction();
		copy.transactionId = that.getTransactionId();
		copy.processorId = that.getProcessorId();
		copy.startPosition = that.getStartPosition();
		copy.endPosition = that.getEndPosition();
		copy.timeout = that.getTimeout();
		copy.detail = that.getDetail();
		copy.state = that.getState();
		copy.startTime = that.getStartTime();
		copy.finishTime = that.getFinishTime();
		copy.attempts = that.getAttempts();
		return copy;
	}
	
	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	public boolean finish(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(this.state);
		if (stateMachine.finish()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
			return true;
		}
		return false;
	}
	
	public boolean abort(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(this.state);
		if (stateMachine.abort()){
			this.finishTime = Instant.now();
			this.state = stateMachine.getState();
			return true;
		}
		return false;
	}
	
	public boolean retry(String processorId, Instant timeout){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(this.state);
		if (stateMachine.retry()){
			this.startTime = Instant.now();
			this.finishTime = null;
			this.state = stateMachine.getState();
			this.attempts ++;
			this.processorId = processorId;
			this.timeout = timeout;
			return true;
		}
		return false;
	}
	
	public boolean timeout(){
		SequentialTransactionStateMachine stateMachine = new SequentialTransactionStateMachine(this.state);
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
	@Override
	public void setStartPosition(String startPosition) {
		this.startPosition = startPosition;
	}
	@Override
	public String getEndPosition() {
		return endPosition;
	}
	@Override
	public void setEndPosition(String endPosition) {
		this.endPosition = endPosition;
	}
	@Override
	public Instant getTimeout() {
		return timeout;
	}
	@Override
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
	public SequentialTransactionState getState() {
		return state;
	}
	public void setState(SequentialTransactionState state) {
		this.state = state;
	}
	@Override
	public Serializable getDetail() {
		return detail;
	}
	@Override
	public void setDetail(Serializable detail) {
		this.detail = detail;
	}
	@Override
	public String getTransactionId() {
		return transactionId;
	}
	@Override
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	@Override
	public String getProcessorId() {
		return processorId;
	}
	@Override
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
