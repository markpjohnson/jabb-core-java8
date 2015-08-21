/**
 * 
 */
package net.sf.jabb.dstream;

/**
 * Data structure for a StreamDataSupplier, an ID, a from position, and a to position.
 * The the from and to positions can be inclusive or exclusive, depending on the upper level application.
 * @author James Hu
 * 
 * @param <M> type of the message object
 *
 */
public class StreamDataSupplierWithIdAndRange<M> extends StreamDataSupplierWithId<M> {
	protected String fromPosition;
	protected String toPosition;
	
	public StreamDataSupplierWithIdAndRange(){
		super();
	}
	
	public StreamDataSupplierWithIdAndRange(String id, StreamDataSupplier<M> supplier){
		super(id, supplier);
	}
	
	public StreamDataSupplierWithIdAndRange(String id, StreamDataSupplier<M> supplier, String fromPosition, String toPosition){
		super(id, supplier);
		this.fromPosition = fromPosition;
		this.toPosition = toPosition;
	}
	

	/**
	 * Get the from position
	 * @return	the from position, can be null
	 */
	public String getFromPosition() {
		return fromPosition;
	}
	
	/**
	 * Set the from position
	 * @param fromPosition		the from position, can be null
	 */
	public void setFromPosition(String fromPosition) {
		this.fromPosition = fromPosition;
	}
	
	/**
	 * Get the to position
	 * @return	the to position, can be null
	 */
	public String getToPosition() {
		return toPosition;
	}
	
	/**
	 * Set the to position
	 * @param toPosition	the to position, can be null
	 */
	public void setToPosition(String toPosition) {
		this.toPosition = toPosition;
	}
}
