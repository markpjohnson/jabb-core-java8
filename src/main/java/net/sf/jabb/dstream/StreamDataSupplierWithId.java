/**
 * Created by mjohnson on 2/29/2016.
 */
package net.sf.jabb.dstream;

import java.time.Instant;

public interface StreamDataSupplierWithId<M> {
	StreamDataSupplierWithIdAndPositionRange<M> withRange(String fromPosition, String toPosition);
	
	StreamDataSupplierWithIdAndEnqueuedTimeRange<M> withRange(Instant fromTime, Instant toTime);
	
	String getId();
	
	void setId(String id);
	
	StreamDataSupplier<M> getSupplier();
	
	void setSupplier(StreamDataSupplier<M> supplier);
}
