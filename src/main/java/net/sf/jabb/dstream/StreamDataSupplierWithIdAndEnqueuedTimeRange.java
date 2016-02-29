/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Instant;
import java.util.function.Function;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

/**
 * Data structure for a StreamDataSupplier, an ID, a from enqueued time, and a to enqueued time.
 * The the from and to enqueued time can be inclusive or exclusive, depending on the upper level application.
 * @author James Hu
 * 
 * @param <M> type of the message object
 *
 */
public class StreamDataSupplierWithIdAndEnqueuedTimeRange<M> extends StreamDataSupplierWithIdImpl<M> implements StreamDataSupplierWithIdAndRange<M, Instant>{
	protected Instant fromEnqueuedTime;
	protected Instant toEnqueuedTime;
	
	public StreamDataSupplierWithIdAndEnqueuedTimeRange(){
		super();
	}
	
	public StreamDataSupplierWithIdAndEnqueuedTimeRange(String id, StreamDataSupplier<M> supplier){
		super(id, supplier);
	}
	
	public StreamDataSupplierWithIdAndEnqueuedTimeRange(String id, StreamDataSupplier<M> supplier, Instant fromEnqueuedTime, Instant toEnqueuedTime){
		super(id, supplier);
		this.fromEnqueuedTime = fromEnqueuedTime;
		this.toEnqueuedTime = toEnqueuedTime;
	}
	
	@Override
	public ReceiveStatus receiveInRange(Function<M, Long> receiver, String startPosition) throws DataStreamInfrastructureException {
		if (startPosition == null || startPosition.length() == 0){
			return supplier.receive(receiver, fromEnqueuedTime, toEnqueuedTime);
		}else{
			return supplier.receive(receiver, startPosition, toEnqueuedTime);
		}
	}
	
	@Override
	public Instant getFrom(){
		return fromEnqueuedTime;
	}

	@Override
	public Instant getTo(){
		return toEnqueuedTime;
	}


	/**
	 * Get the from enqueued time
	 * @return	the from enqueued time, can be null
	 */
	public Instant getFromEnqueuedTime() {
		return fromEnqueuedTime;
	}
	
	/**
	 * Set the from enqueued time
	 * @param fromEnqueuedTime		the from enqueued time, can be null
	 */
	public void setFromPosition(Instant fromEnqueuedTime) {
		this.fromEnqueuedTime = fromEnqueuedTime;
	}
	
	/**
	 * Get the to enqueued time
	 * @return	the to enqueued time, can be null
	 */
	public Instant getToPosition() {
		return toEnqueuedTime;
	}
	
	/**
	 * Set the to enqueued time
	 * @param toEnqueuedTime	the to enqueued time, can be null
	 */
	public void setToPosition(Instant toEnqueuedTime) {
		this.toEnqueuedTime = toEnqueuedTime;
	}
}
