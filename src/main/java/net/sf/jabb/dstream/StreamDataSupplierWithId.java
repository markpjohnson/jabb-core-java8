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
	
	public StreamDataSupplierWithIdAndPositionRange<M> withRange(String fromPosition, String toPosition){
		if (fromPosition != null && toPosition != null){
			Validate.isTrue(supplier.isInRange(fromPosition, toPosition), "fromPosition cannot be after toPosition");
		}
		return new StreamDataSupplierWithIdAndPositionRange<>(id, supplier, fromPosition, toPosition);
	}
	
	public StreamDataSupplierWithIdAndEnqueuedTimeRange<M> withRange(Instant fromTime, Instant toTime){
		if (fromTime != null && toTime != null){
			Validate.isTrue(supplier.isInRange(fromTime, toTime), "fromTime cannot be after toTime");
		}
		return new StreamDataSupplierWithIdAndEnqueuedTimeRange<>(id, supplier, fromTime, toTime);
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
