/**
 * 
 */
package net.sf.jabb.txprogress;

import java.io.Serializable;
import java.time.Instant;

/**
 * Transaction of processing.
 * @author James Hu
 *
 */
public interface ProgressTransaction {
	
	/**
	 * Get the ID of this transaction
	 * @return	ID of the transaction
	 */
	String getTransactionId();
	
	/**
	 * Get the ID of the processor that owns the transaction
	 * @return	ID of the owner processor
	 */
	String getProcessorId();

	/**
	 * Get the start position of the transaction
	 * @return	the start position, for example the sequence number in input data stream
	 */
	String getStartPosition();
	
	/**
	 * Get the end position of the transaction
	 * @return	the end position, for example the sequence number in input data stream
	 */
	String getEndPosition();
	
	/**
	 * Get the time that the transaction will time out
	 * @return	the time that the transaction will time out
	 */
	Instant getTimeout();
	
	/**
	 * Get the time that this transaction started
	 * @return the start time of the transaction
	 */
	Instant getStartTime();
	
	/**
	 * Get the time that this transaction finished or aborted
	 * @return	the finish or abort time of the transaction. If the transaction timed out, return null.
	 */
	Instant getFinishTime();
	
	/**
	 * Get the state of the transaction
	 * @return	the state
	 */
	ProgressTransactionState getState();
	
	/**
	 * Get the details of the transaction
	 * @return	the details
	 */
	Serializable getTransaction();
	
	/**
	 * Get the number of attempts for the transaction
	 * @return 1 if the transaction has been attempted once, 
	 * 			2 if the transaction has failed once and later been retried once, 
	 * 			3 if the transaction has been tried and retried three times, etc.
	 */
	int getAttempts();
	
	default boolean isInProgress(){
		return ProgressTransactionState.IN_PROGRESS.equals(getState());
	}
	
	default boolean isFinished(){
		return ProgressTransactionState.FINISHED.equals(getState());
	}
	
	default boolean isFailed(){
		ProgressTransactionState state = getState();
		return ProgressTransactionState.ABORTED.equals(state) || ProgressTransactionState.TIMED_OUT.equals(state);
	}

}
