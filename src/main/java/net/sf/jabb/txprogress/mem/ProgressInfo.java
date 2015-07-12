/**
 * 
 */
package net.sf.jabb.txprogress.mem;

import java.time.Instant;

import net.sf.jabb.txprogress.ProgressTransaction;

/**
 * Data structure to hold information about a progress
 * @author James Hu
 *
 */
public class ProgressInfo {
	protected Object lock = new Object();
	protected String leasedByProcessor;
	protected Instant leaseStartTime;
	protected Instant leaseExpirationTime;
	protected ProgressTransaction lastSucceededTransaction;
	protected ProgressTransaction currentTransaction;
	
	/**
	 * Update the lease. The start time of the lease will be Instant.now() or null if the leaseExpirationTimed argument is null 
	 * @param processorId	ID of the processor, can be null
	 * @param leaseExpirationTimed	expiration time, can be null
	 */
	public void updateLease(String processorId, Instant leaseExpirationTimed){
		this.leasedByProcessor = processorId;
		this.leaseStartTime = leaseExpirationTime == null ? null : Instant.now();
		this.leaseExpirationTime = leaseExpirationTimed;
	}
	
	public Object getLock() {
		return lock;
	}
	public String getLeasedByProcessor() {
		return leasedByProcessor;
	}
	public void setLeasedByProcessor(String leasedByProcessor) {
		this.leasedByProcessor = leasedByProcessor;
	}
	public Instant getLeaseStartTime() {
		return leaseStartTime;
	}
	public void setLeaseStartTime(Instant leaseStartTime) {
		this.leaseStartTime = leaseStartTime;
	}
	public Instant getLeaseExpirationTime() {
		return leaseExpirationTime;
	}
	public void setLeaseExpirationTime(Instant leaseExpirationTime) {
		this.leaseExpirationTime = leaseExpirationTime;
	}
	public ProgressTransaction getLastSucceededTransaction() {
		return lastSucceededTransaction;
	}
	public void setLastSucceededTransaction(ProgressTransaction lastSucceededTransaction) {
		this.lastSucceededTransaction = lastSucceededTransaction;
	}
	public ProgressTransaction getCurrentTransaction() {
		return currentTransaction;
	}
	public void setCurrentTransaction(ProgressTransaction currentTransaction) {
		this.currentTransaction = currentTransaction;
	}
}
