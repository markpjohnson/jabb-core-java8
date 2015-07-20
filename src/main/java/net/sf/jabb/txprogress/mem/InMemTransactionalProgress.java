/**
 * 
 */
package net.sf.jabb.txprogress.mem;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jabb.txprogress.BasicProgressTransaction;
import net.sf.jabb.txprogress.ProgressTransaction;
import net.sf.jabb.txprogress.ProgressTransactionState;
import net.sf.jabb.txprogress.ProgressTransactionStateMachine;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.txprogress.ex.NotCurrentTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningLeaseException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;
import net.sf.jabb.txprogress.ex.TransactionTimeoutAfterLeaseExpirationException;
import net.sf.jabb.util.col.PutIfAbsentMap;

/**
 * Transient transaction progress track with all data kept in memory.
 * This implementation is intended for testing, PoC, and demo usage.
 * @author James Hu
 *
 */
public class InMemTransactionalProgress implements TransactionalProgress {
	
	protected Map<String, LinkedList<ProgressTransaction>> progresses;
	
	public InMemTransactionalProgress(){
		progresses = new PutIfAbsentMap<String, LinkedList<ProgressTransaction>>(new HashMap<String, LinkedList<ProgressTransaction>>(), k->new LinkedList<>());
	}

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
	
	/**
	 * Check if the processor currently own a valid lease, and check if a transaction time out is after the lease expiration time.
	 * @param progress		the progress
	 * @param processorId	ID of the processor
	 * @param transactionTimeout time out of a transaction
	 * @throws NotOwningLeaseException	if the processor does not currently own a valid lease
	 * @throws TransactionTimeoutAfterLeaseExpirationException if the transaction time out is after lease expiration time
	 */
	protected void validateLease(ProgressInfo progress, String processorId, Instant transactionTimeout) throws NotOwningLeaseException, TransactionTimeoutAfterLeaseExpirationException{
		validateLease(progress, processorId);
		Instant expiration = progress.getLeaseExpirationTime();
		if (transactionTimeout.isAfter(expiration)){
			throw new TransactionTimeoutAfterLeaseExpirationException("transactionTimeout=" + transactionTimeout + ", leaseExpirationTime=" + expiration);
		}
	}
	
	/**
	 * Check if the processor currently own a valid lease
	 * @param progress		the progress
	 * @param processorId	ID of the processor
	 * @throws NotOwningLeaseException	if the processor does not currently own a valid lease
	 */
	protected void validateLease(ProgressInfo progress, String processorId) throws NotOwningLeaseException{
		if (!processorId.equals(progress.getLeasedByProcessor())){ 	// leased by the another processor
			throw new NotOwningLeaseException();
		}
		Instant expiration = progress.getLeaseExpirationTime();
		if(expiration == null || expiration.isBefore(Instant.now())){		// the lease has expired
			throw new NotOwningLeaseException();
		}
	}

	@Override
	public boolean renewLease(String progressId, String processorId, Instant leaseExpirationTimed) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			try {
				validateLease(progress, processorId);
			} catch (NotOwningLeaseException e) {
				return false;
			}
			progress.setLeaseExpirationTime(leaseExpirationTimed);
			return true;
		}
	}

	@Override
	public void releaseLease(String progressId, String processorId) throws NotOwningLeaseException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId);
			progress.updateLease(null, null);
		}
	}

	@Override
	public String getProcessor(String progressId) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			String processorId = progress.getLeasedByProcessor();
			if (processorId == null){
				return null;
			}else{
				Instant expirationTime = progress.getLeaseExpirationTime();
				if (expirationTime != null && expirationTime.isAfter(Instant.now())){
					return processorId;
				}else{
					return null;
				}
			}
		}
	}
	
	/**
	 * Check if the last/current transaction is successful. 
	 * It will move currentTransaction to lastSucceededTransaction if it succeeded.
	 * It will change the state of currentTransaction to timed out if it timed out.
	 * @param progress	the progress information
	 * @return	true if a new transaction can be started; false if current transaction must be retried
	 * @throws IllegalTransactionStateException 
	 */
	protected boolean checkLastSuccessfulTransaction(ProgressInfo progress){
		synchronized(progress.getLock()){
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction == null){
				return true;
			}else{
				if (ProgressTransactionState.FINISHED.equals(currentTransaction.getState())){
					progress.setLastSucceededTransaction(currentTransaction);
					progress.setCurrentTransaction(null);
					return true;
				}else if (ProgressTransactionState.IN_PROGRESS.equals(currentTransaction.getState()) 
						&& currentTransaction.getTimeout().isBefore(Instant.now())){	// timed out
					if (currentTransaction.timeout()){
						// done
					}else{
						// this should never happen because we already checked current status must be IN_PROGRESS
						throw new IllegalStateException("Cannot time out transaction " + currentTransaction.getTransactionId() + " from state " + currentTransaction.getState());
					}
					return false;
				}else{
					return false;
				}
				
			}
		}
	}
	
	@Override
	public String startTransaction(String progressId, String processorId, String startPosition,
			String endPosition, Instant timeout, Serializable transaction,
			String transactionId) throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, TransactionTimeoutAfterLeaseExpirationException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId, timeout);
			if (checkLastSuccessfulTransaction(progress)){
				BasicProgressTransaction currentTransaction = new BasicProgressTransaction(transactionId, processorId, startPosition, endPosition, timeout, transaction);
				progress.setCurrentTransaction(currentTransaction);
				return transactionId;
			}else{
				throw new LastTransactionIsNotSuccessfulException();	// must retry and finish the last one
			}
		}
	}

	@Override
	public void finishTransaction(String progressId, String processorId, String transactionId, String endPosition) 
			throws NotOwningTransactionException, IllegalTransactionStateException, NotCurrentTransactionException, NotOwningLeaseException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId);
			checkLastSuccessfulTransaction(progress); // it handles time out
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					if (currentTransaction.finish()){
						if (endPosition != null){
							currentTransaction.setEndPosition(endPosition);
						}
						progress.setLastSucceededTransaction(currentTransaction);
						progress.setCurrentTransaction(null);
					}else{
						throw new IllegalTransactionStateException("Cannot finish transaction " + transactionId + " from state " + currentTransaction.getState());
					}
				}else{ // it is not owned by the processor
					throw new NotOwningTransactionException();
				}
			}else{ // it is not the current transaction
				throw new NotCurrentTransactionException();
			}
		}
	}

	@Override
	public void abortTransaction(String progressId, String processorId, String transactionId, String endPosition) 
			throws NotOwningTransactionException, IllegalTransactionStateException, NotCurrentTransactionException, NotOwningLeaseException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId);
			checkLastSuccessfulTransaction(progress); // it handles time out
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					if (currentTransaction.abort()){
						if (endPosition != null){
							currentTransaction.setEndPosition(endPosition);
						}
					}else{
						throw new IllegalTransactionStateException("Cannot abort transaction " + transactionId + " from state " + currentTransaction.getState());
					}
				}else{ // it is not owned by the processor
					throw new NotOwningTransactionException();
				}
			}else{ // it is not the current transaction
				throw new NotCurrentTransactionException();
			}
		}
	}

	@Override
	public ProgressTransaction retryLastUnsuccessfulTransaction(String progressId, String processorId, Instant transactionTimeout) 
			throws NotOwningTransactionException, NotOwningLeaseException, TransactionTimeoutAfterLeaseExpirationException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId, transactionTimeout);
			if (checkLastSuccessfulTransaction(progress)){ // it handles time out
				return null;	// no transaction needs to be retried
			}else{
				BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
				if (currentTransaction.retry()){
					currentTransaction.setProcessorId(processorId);
					currentTransaction.setTimeout(transactionTimeout);
					return currentTransaction;
				}else{
					// this should never happen because we already checked
					throw new IllegalStateException("Cannot retry transaction " + currentTransaction.getTransactionId() + " from state " + currentTransaction.getState());
				}
			}
		}
	}

	@Override
	public void renewTransactionTimeout(String progressId, String processorId, String transactionId, Instant transactionTimeout) 
			throws NotOwningTransactionException, NotOwningLeaseException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			validateLease(progress, processorId, transactionTimeout);
			checkLastSuccessfulTransaction(progress); // it handles time out
			BasicProgressTransaction currentTransaction = (BasicProgressTransaction) progress.getCurrentTransaction();
			if (currentTransaction != null && currentTransaction.getTransactionId().equals(transactionId)){
				if (currentTransaction.getProcessorId().equals(processorId)){
					currentTransaction.setTimeout(transactionTimeout);
				}else{ // it is not owned by the processor
					throw new NotOwningTransactionException();
				}
			}else{ // it is not the current transaction
				throw new NotCurrentTransactionException();
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#isTransactionSuccessful(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean isTransactionSuccessful(String progressId, String transactionId, Instant beforeWhen) {
		ProgressInfo progress = progresses.get(progressId);
		synchronized(progress.getLock()){
			ProgressTransaction currentTransaction = progress.getCurrentTransaction();
			ProgressTransaction lastTransaction = progress.getLastSucceededTransaction();
			
			if (lastTransaction != null){
				Instant lastFinishTime = lastTransaction.getFinishTime();
				if (lastFinishTime != null && !beforeWhen.isAfter(lastFinishTime) 
						&& (currentTransaction == null || !transactionId.equals(currentTransaction.getTransactionId()))){	// happened before the finish time of last finished transaction
					return true;
				}else if (transactionId.equals(lastTransaction.getTransactionId())){	// the same transaction as last succeeded
					return true;
				}
			}
			
			// no last succeeded transaction or not able to determine by checking last succeeded transaction
			if (currentTransaction != null && transactionId.equals(currentTransaction.getTransactionId())){
				return ProgressTransactionState.FINISHED.equals(currentTransaction.getState());  // just in case the current one just succeeded
			}else{
				return true;	// id does not match either last succeeded or current, so the transaction must have been succeeded 
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.transprogtracker.TransactionalProgressTracker#getLastSuccessfulTransaction(java.lang.String)
	 */
	@Override
	public ProgressTransaction getLastSuccessfulTransaction(String progressId) {
		ProgressInfo progress = progresses.get(progressId);
		checkLastSuccessfulTransaction(progress); // it handles time out and is synchronized inside
		ProgressTransaction lastTransaction = progress.getLastSucceededTransaction();
		return lastTransaction;
	}
	
	/**
	 * Remove succeeded from the head and leave only one
	 * @param transactions	 the list of transactions
	 */
	protected void compact(LinkedList<ProgressTransaction> transactions){
		Iterator<ProgressTransaction> iterator = transactions.iterator();
		if (!iterator.hasNext()){
			return;
		}
		
		ProgressTransaction tx = iterator.next();
		if (!ProgressTransactionState.FINISHED.equals(tx.getState())){
			return;
		}
		
		while(iterator.hasNext()){
			tx = iterator.next();
			if (ProgressTransactionState.FINISHED.equals(tx.getState())){
				transactions.removeFirst();
			}
		}
	}
	
	static class TransactionCounts{
		int retryingCount;
		int inProgressCount;
		int failedCount;
	}
	
	protected TransactionCounts getCounts(LinkedList<ProgressTransaction> transactions){
		TransactionCounts counts = new TransactionCounts();
		
		for (ProgressTransaction tx: transactions){
			switch(tx.getState()){
				case IN_PROGRESS:
					counts.inProgressCount ++;
					if (tx.getAttempts() > 1){
						counts.retryingCount ++;
					}
					break;
				case FINISHED:
					break;
				default:	// failed
					counts.failedCount ++;
					break;
			}
		}
		return counts;
	}

	@Override
	public ProgressTransaction startTransaction(String progressId,
			String processorId, int maxInProgressTransacions,
			int maxRetryingTransactions) throws InfrastructureErrorException {
		LinkedList<ProgressTransaction> transactions = progresses.get(progressId);
		synchronized(transactions){
			compact(transactions);
			TransactionCounts counts = getCounts(transactions);
			if (counts.inProgressCount >= maxInProgressTransacions){	// no more transaction allowed
				return null;
			}
			if (counts.retryingCount < maxRetryingTransactions && counts.failedCount > 0){	// pick up a failed to retry
				
			}else{	// a new transaction
				
			}
		}
		return null;
	}

	@Override
	public ProgressTransaction startTransaction(String progressId,
			ProgressTransaction transaction, int maxInProgressTransacions,
			int maxRetryingTransactions) throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void finishTransaction(String progressId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abortTransaction(String progressId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<ProgressTransaction> getRecentTransactions(String progressId)
			throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return null;
	}

}
