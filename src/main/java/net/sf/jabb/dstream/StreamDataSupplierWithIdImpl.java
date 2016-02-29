/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Instant;

import org.apache.commons.lang3.Validate;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;


/**
 * Data structure for a StreamDataSupplier and an ID.
 * @author James Hu
 *
 * @param <M> type of the message object
 */
public class StreamDataSupplierWithIdImpl<M> implements StreamDataSupplierWithId<M> {
	protected String id;
	protected StreamDataSupplier<M> supplier;
	
	public StreamDataSupplierWithIdImpl(){
		
	}
	
	public StreamDataSupplierWithIdImpl(String id, StreamDataSupplier<M> supplier){
		this.id = id;
		this.supplier = supplier;
	}
	
	@Override
	public StreamDataSupplierWithIdAndPositionRange<M> withRange(String fromPosition, String toPosition){
		if (fromPosition != null && toPosition != null){
			Validate.isTrue(supplier.isInRange(fromPosition, toPosition), "fromPosition cannot be after toPosition");
		}
		return new StreamDataSupplierWithIdAndPositionRange<>(id, supplier, fromPosition, toPosition);
	}
	
	@Override
	public StreamDataSupplierWithIdAndEnqueuedTimeRange<M> withRange(Instant fromTime, Instant toTime){
		if (fromTime != null && toTime != null){
			Validate.isTrue(supplier.isInRange(fromTime, toTime), "fromTime cannot be after toTime");
		}
		return new StreamDataSupplierWithIdAndEnqueuedTimeRange<>(id, supplier, fromTime, toTime);
	}
	
	@Override
	public String toString(){
		return (id == null ? "" : id) + ": " + supplier;
	}
	
	@Override
	public String getId() {
		return id;
	}
	@Override
	public void setId(String id) {
		this.id = id;
	}
	@Override
	public StreamDataSupplier<M> getSupplier() {
		return supplier;
	}
	@Override
	public void setSupplier(StreamDataSupplier<M> supplier) {
		this.supplier = supplier;
	}
	
}
