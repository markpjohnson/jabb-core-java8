/**
 * 
 */
package net.sf.jabb.txprogress;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * Partially modifiable version of the progress transaction.
 * @author James Hu
 *
 */
public interface ProgressTransaction extends ReadOnlyProgressTransaction{
	/**
	 * Set the proposed ID of this transaction
	 * @param proposedTransactionId	proposed ID of the transaction
	 */
	void setTransactionId(String proposedTransactionId);
	
	/**
	 * Set the ID of the processor that owns the transaction
	 * @param processorId	ID of the owner processor
	 */
	void setProcessorId(String processorId);

	/**
	 * Set the start position of the transaction
	 * @param startPosition	the start position, for example the sequence number in input data stream
	 */
	void setStartPosition(String startPosition);
	
	/**
	 * Set the end position of the transaction
	 * @param endPosition	the end position, for example the sequence number in input data stream
	 */
	void setEndPosition(String endPosition);
	
	/**
	 * Set the time that the transaction will time out
	 * @param timeout	the time that the transaction will time out
	 */
	void setTimeout(Instant timeout);
	
	/**
	 * Set the time that the transaction will time out
	 * @param timeoutDuration	the duration after which the transaction will time out
	 */
	default void setTimeout(Duration timeoutDuration){
		setTimeout(Instant.now().plus(timeoutDuration));
	}
	
	/**
	 * Set the details of the transaction
	 * @param detail	the detail
	 */
	void setDetail(Serializable detail);
	

}
