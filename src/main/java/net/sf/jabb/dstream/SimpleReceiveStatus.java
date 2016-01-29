/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Instant;

/**
 * @author James Hu
 *
 */
public class SimpleReceiveStatus implements ReceiveStatus {
	protected String lastPosition;
	protected Instant lastEnqueuedTime;
	protected boolean outOfRangeReached;
	
	public SimpleReceiveStatus(){
		this(null, null, false);
	}
	
	public SimpleReceiveStatus(String lastPosition, Instant lastEnqueuedTime, boolean outOfRangeReached){
		this.lastPosition = lastPosition;
		this.lastEnqueuedTime = lastEnqueuedTime;
		this.outOfRangeReached = outOfRangeReached;
	}
	
	@Override
	public String toString(){
		return "(" + lastPosition + ", " + lastEnqueuedTime + ", " + outOfRangeReached + ")";
	}
	
	/**
	 * @return the lastPosition
	 */
	@Override
	public String getLastPosition() {
		return lastPosition;
	}
	/**
	 * @param lastPosition the lastPosition to set
	 */
	public void setLastPosition(String lastPosition) {
		this.lastPosition = lastPosition;
	}
	/**
	 * @return the lastEnqueuedTime
	 */
	@Override
	public Instant getLastEnqueuedTime() {
		return lastEnqueuedTime;
	}
	/**
	 * @param lastEnqueuedTime the lastEnqueuedTime to set
	 */
	public void setLastEnqueuedTime(Instant lastEnqueuedTime) {
		this.lastEnqueuedTime = lastEnqueuedTime;
	}
	/**
	 * @return the outOfRangeReached
	 */
	@Override
	public boolean isOutOfRangeReached() {
		return outOfRangeReached;
	}
	/**
	 * @param outOfRangeReached the outOfRangeReached to set
	 */
	public void setOutOfRangeReached(boolean outOfRangeReached) {
		this.outOfRangeReached = outOfRangeReached;
	}

}
