/**
 * 
 */
package net.sf.jabb.dstream;

import java.util.function.Function;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

/**
 * @author James Hu
 * @param <M> type of the message
 * @param <R> type of the range
 */
public interface StreamDataSupplierWithIdAndRange<M, R> {
	/**
	 * Get the ID of the stream+range. Useful for logging
	 * @return	the ID
	 */
	String getId();
	
	/**
	 * Get the stream data supplier
	 * @return	the stream data supplier
	 */
	StreamDataSupplier<M> getSupplier();
	
	/**
	 * Receive data from the supplier within range. Reference: {@link StreamDataSupplier#receive(Function, String, java.time.Instant)}
	 * @param receiver	the receiver
	 * @param startPosition	the start position for the receiving, if it is null or empty string then the from position of the range will be used
	 * @return	the receive status
	 * @throws DataStreamInfrastructureException  if exception happens in the infrastructure
	 */
	ReceiveStatus receiveInRange(Function<M, Long> receiver, String startPosition) throws DataStreamInfrastructureException;

	/**
	 * Get the from 
	 * @return the from
	 */
	R getFrom();
	
	/**
	 * Get the to
	 * @return the to
	 */
	R getTo();
}
