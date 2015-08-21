/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Duration;
import java.time.Instant;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

import org.apache.commons.lang3.Validate;


/**
 * Data structure for a StreamDataSupplier and an ID.
 * @author James Hu
 *
 * @param <M> type of the message object
 */
public class StreamDataSupplierWithId<M> {
	protected String id;
	protected StreamDataSupplier<M> supplier;
	
	public StreamDataSupplierWithId(){
		
	}
	
	public StreamDataSupplierWithId(String id, StreamDataSupplier<M> supplier){
		this.id = id;
		this.supplier = supplier;
	}
	
	public StreamDataSupplierWithIdAndRange<M> withRange(String fromPosition, String toPosition){
		return new StreamDataSupplierWithIdAndRange<>(id, supplier, fromPosition, toPosition);
	}
	
	public StreamDataSupplierWithIdAndRange<M> withRange(Instant fromTime, Instant toTime, Duration waitForArrival) throws InterruptedException, DataStreamInfrastructureException{
		if (fromTime != null && toTime != null){
			Validate.isTrue(!fromTime.isAfter(toTime), "fromTime cannot be after toTime");
		}
		StreamDataSupplierWithIdAndRange<M> result = new StreamDataSupplierWithIdAndRange<>(id, supplier);
		if (fromTime == null && toTime == null){
			return result;
		}else{
			String firstPosition;
			Instant firstTime;
			String lastPosition;
			Instant lastTime;
			if (fromTime != null){
				String fromPosition = supplier.firstPosition(fromTime, waitForArrival);
				if (fromPosition != null){
					result.setFromPosition(fromPosition);
					if (toTime == null){
						return result;
					}else{
						String toPosition = supplier.firstPosition(toTime, waitForArrival);
						if (toPosition != null){
							result.setToPosition(toPosition);
							return result;
						}else{
							// assume toTime < now
							lastPosition = supplier.lastPosition();	// must not be null
							result.setToPosition(lastPosition);
							return result;
						}
					}
				}else{
					lastPosition = supplier.lastPosition();
					if (lastPosition != null){
						lastTime = supplier.enqueuedTime(lastPosition);
						firstPosition = supplier.firstPosition(Instant.EPOCH, waitForArrival);
						firstTime = supplier.enqueuedTime(firstPosition);
						if (fromTime.isBefore(firstTime)){   // ( , firstTime)
							result.setFromPosition(supplier.firstPosition());
							if (toTime == null){
								return result;
							}else{
								String toPosition = supplier.firstPosition(toTime, waitForArrival);
								if (toPosition != null){
									result.setToPosition(toPosition);
									return result;
								}else{
									if (toTime.isBefore(lastTime)){
										throw new IllegalStateException("Failed to get the position corresponding to toTime '" + toTime + "'");
									}else{
										result.setToPosition(lastPosition);
										return result;
									}
								}
							}
						}else if (fromTime.isBefore(lastTime)){  // [firstTime, lastTime)
							throw new IllegalStateException("Failed to get the position corresponding to fromTime '" + fromTime + "'");
						}else{  // [lastTime, )
							if (toTime == null){	// all in the future
								result.setFromPosition(lastPosition);
								return result;
							}else{
								return null;  // nothing (assuming toTime < now)
							}
						}
					}else{		// no message in the queue
						return null;
					}
				}
			}else{ //fromTime == null
				result.setFromPosition(supplier.firstPosition());
				String toPosition = supplier.firstPosition(toTime, waitForArrival);
				if (toPosition != null){
					result.setToPosition(toPosition);
					return result;
				}else{
					lastPosition = supplier.lastPosition();
					if (lastPosition != null){
						lastTime = supplier.enqueuedTime(lastPosition);
						if (toTime.isBefore(lastTime)){
							throw new IllegalStateException("Failed to get the position corresponding to toTime '" + toTime + "'");
						}else{
							result.setToPosition(lastPosition);
							return result;
						}
					}else{
						return null;	// no message
					}
				}
			}
		}
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public StreamDataSupplier<M> getSupplier() {
		return supplier;
	}
	public void setSupplier(StreamDataSupplier<M> supplier) {
		this.supplier = supplier;
	}
	
}
