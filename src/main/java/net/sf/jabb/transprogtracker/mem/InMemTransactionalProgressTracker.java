/**
 * 
 */
package net.sf.jabb.transprogtracker.mem;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.jabb.transprogtracker.BasicProgressTransaction;
import net.sf.jabb.transprogtracker.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.transprogtracker.NotOwningLeaseException;
import net.sf.jabb.transprogtracker.NotOwningTransactionException;
import net.sf.jabb.transprogtracker.ProgressTransaction;
import net.sf.jabb.transprogtracker.ProgressTransactionState;
import net.sf.jabb.transprogtracker.ProgressTransactionStateMachine;
import net.sf.jabb.transprogtracker.TransactionalProgressTracker;
import net.sf.jabb.util.bean.DoubleValueBean;
import net.sf.jabb.util.col.PutIfAbsentMap;

/**
 * Transient transaction progress track with all data kept in memory.
 * This implementation is intended for testing, PoC, and demo usage.
 * @author James Hu
 *
 */
public class InMemTransactionalProgressTracker implements TransactionalProgressTracker {
	
	protected Map<String, ProgressInfo> progresses;
	
	public InMemTransactionalProgressTracker(){
		progresses = new PutIfAbsentMap<String, ProgressInfo>(new HashMap<String, ProgressInfo>(), ProgressInfo.class);
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#acquireLease(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean acquireLease(String progressId, String processorId, Instant leaseExpirationTimed) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			if (progress.getLeasedByProcessor() == null 		// not on lease
					|| progress.getLeasedByProcessor().equals(processorId) 	// leased by the same processor
					|| progress.getLeaseExpirationTime().isBefore(Instant.now())){	// lease expired
				progress.updateLease(processorId, leaseExpirationTimed);
				return true;
			}else{
				// failed to acquire the lease
				return false;
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#renewLease(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean renewLease(String progressId, String processorId, Instant leaseExpirationTimed) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			if (processorId.equals(progress.getLeasedByProcessor()) 	// leased by the same processor
					&& progress.getLeaseExpirationTime().isAfter(Instant.now())){		// the lease has not expired
				progress.setLeaseExpirationTime(leaseExpirationTimed);
				return true;
			}else{
				return false;
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#releaseLease(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean releaseLease(String progressId, String processorId) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			if (progress.getLeasedByProcessor() == null){			// not on lease
				return true;
			}else if (!processorId.equals(progress.getLeasedByProcessor()) 	// leased by others
					&& progress.getLeaseExpirationTime().isAfter(Instant.now())){	// the lease has not expired
				return false;
			}else{
				progress.updateLease(null, null);
				return true;
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#getProcessor(java.lang.String)
	 */
	@Override
	public String getProcessor(String progressId) {
		return progresses.get(progressId).getLeasedByProcessor();
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#startTransaction(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.time.Instant, java.io.Serializable, java.lang.String)
	 */
	@Override
	public String startTransaction(String progressId, String processorId, String startPosition,
			String endPosition, Instant timeout, Serializable transaction,
			String transactionId) throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			if (processorId.equals(progress.getLeasedByProcessor()) 	// leased by the same processor
					&& progress.getLeaseExpirationTime().isAfter(Instant.now())){		// the lease has not expired
				ProgressTransaction currentTransaction = progress.getCurrentTransaction();
				if (currentTransaction != null){
					if (ProgressTransactionState.FINISHED.equals(currentTransaction.getState())){
						progress.setLastSucceededTransaction(currentTransaction);
					}else{
						throw new LastTransactionIsNotSuccessfulException();
					}
				}
				currentTransaction = new BasicProgressTransaction(transactionId, processorId, startPosition, endPosition, timeout, transaction);
				progress.setCurrentTransaction(currentTransaction);
				return transactionId;
			}else{
				throw new NotOwningLeaseException();
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#finishTransaction(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void finishTransaction(String progressId, String processorId, String transactionId) throws NotOwningTransactionException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(currentTransaction.getState());
					if (stateMachine.finish()){
						currentTransaction.setState(stateMachine.getState());
						progress.setLastSucceededTransaction(currentTransaction);
						progress.setCurrentTransaction(null);
					}
				}else{
					throw new NotOwningTransactionException();
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#abortTransaction(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void abortTransaction(String progressId, String processorId, String transactionId) throws NotOwningTransactionException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(currentTransaction.getState());
					if (stateMachine.abort()){
						currentTransaction.setState(stateMachine.getState());
					}
				}else{
					throw new NotOwningTransactionException();
				}
			}
		}
	}

	@Override
	public void retryTransaction(String progressId, String processorId, String transactionId) throws NotOwningTransactionException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					ProgressTransactionStateMachine stateMachine = new ProgressTransactionStateMachine(currentTransaction.getState());
					if (stateMachine.retry()){
						currentTransaction.setState(stateMachine.getState());
					}
				}else{
					throw new NotOwningTransactionException();
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#renewTransactionTimeout(java.lang.String, java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public void renewTransactionTimeout(String progressId, String processorId, String transactionId, Instant timeout) throws NotOwningTransactionException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					currentTransaction.setTimeout(timeout);
				}else{
					throw new NotOwningTransactionException();
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#isTransactionSuccessful(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean isTransactionSuccessful(String progressId, String transactionId, Instant beforeWhen) {
		ProgressInfo progress = progresses.get(progressId);
		ProgressTransaction currentTransaction = progress.getCurrentTransaction();
		ProgressTransaction lastTransaction = progress.getLastSucceededTransaction();
		return false;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#getLastSuccessfulTransaction(java.lang.String)
	 */
	@Override
	public ProgressTransaction getLastSuccessfulTransaction(String progressId) {
		ProgressInfo progress = progresses.get(progressId);
		ProgressTransaction lastTransaction = progress.getLastSucceededTransaction();
		return lastTransaction;
	}

}
