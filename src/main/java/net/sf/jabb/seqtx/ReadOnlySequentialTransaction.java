/**
 * 
 */
package net.sf.jabb.seqtx;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.Instant;

/**
 * Read-only version of the progress transaction.
 * @author James Hu
 *
 */
public interface ReadOnlySequentialTransaction {
	
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
	SequentialTransactionState getState();
	
	/**
	 * Get the detail of the transaction
	 * @return	the detail
	 */
	Serializable getDetail();
	
	/**
	 * Get the number of attempts for the transaction
	 * @return 1 if the transaction has been attempted once, 
	 * 			2 if the transaction has failed once and later been retried once, 
	 * 			3 if the transaction has been tried and retried three times, etc.
	 */
	int getAttempts();
	
	default boolean isInProgress(){
		return SequentialTransactionState.IN_PROGRESS.equals(getState());
	}
	
	default boolean isFinished(){
		return SequentialTransactionState.FINISHED.equals(getState());
	}
	
	default boolean isFailed(){
		SequentialTransactionState state = getState();
		return SequentialTransactionState.ABORTED.equals(state) || SequentialTransactionState.TIMED_OUT.equals(state);
	}
	
	default boolean hasStarted(){
		return getStartTime() != null;
	}
	
	default Long getStartPositionAsLong(){
		String s = getStartPosition();
		return s == null ? null : Long.valueOf(s);
	}

	default BigInteger getStartPositionAsBigInteger(){
		String s = getStartPosition();
		return s == null ? null : new BigInteger(s);
	}

	default Long getEndPositionAsLong(){
		String s = getEndPosition();
		return s == null ? null : Long.valueOf(s);
	}

	default BigInteger getEndPositionAsBigInteger(){
		String s = getEndPosition();
		return s == null ? null : new BigInteger(s);
	}

	default String getDetailAsString(){
		return (String)getDetail();
	}

	default Integer getDetailAsInteger(){
		return (Integer)getDetail();
	}
	
	default Long getDetailAsLong(){
		return (Long)getDetail();
	}
}
