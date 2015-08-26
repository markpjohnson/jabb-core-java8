/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Instant;

/**
 * The status of receive/fetch operation.
 * @author James Hu
 *
 */
public interface ReceiveStatus {
	/**
	 * Get the position of the last one within range received
	 * @return	position of the last one within range received
	 */
	String getLastPosition();
	
	/**
	 * Get the enqueued time of the last one within range received
	 * @return enqueued time of the last one within range received
	 */
	Instant getLastEnqueuedTime();
	
	/**
	 * Whether during receiving/fetching, an out of range message/data item had been reached
	 * @return true if during receiving/fetching, a message that is beyond the end position/enqueued time had been reached, false otherwise.
	 */
	boolean isOutOfRangeReached();
}
