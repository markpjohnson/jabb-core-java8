/**
 * 
 */
package net.sf.jabb.txprogress;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import net.sf.jabb.txprogress.ex.DuplicatedTransactionIdException;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.NoSuchTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;

/**
 * Tracker of transactional processing progress.
 * For a progress, there can be more than one transactions initiated by different processors in progress.
 * Therefore, there is no need for processors to acquire and hold leases on progresses.
 * However, all transactions of any progress are continuous.
 * 
 * For any transaction, there is always
 * a previous transaction (unless it is the first transaction) and there is no gap or overlap between its startPosition 
 * and the endPosition of the previous transaction.
 * For any transaction, there is always
 * a next transaction (unless it is the last transaction) and there is no gap or overlap between its endPosition 
 * and the startPosition of the next transaction.
 * 
 * <ul>
 * 	<li>ID of the transaction: They can be generated by the service and can also be specified by the client. They are NOT guaranteed to be unique across progresses.</li>
 * </ul>
 * 
 * Usage example:
 * <pre>
 * TransactionalProgress progressTracker = new ...;
 * while(true){
 * 		ProgressTransaction tx = progressTracker.startTransaction(progressId, processorId, 5, 5);
 * 		while (tx != null && tx.getTransactionId() == null){
 * 			tx.setStartPosition(getNextAvailableBatchStart(tx.getStartPosition()));
 * 			tx.setEndPosition(getNextAvailableBatchEnd(tx.getStartPosition()));
 * 			tx = progressTracker.startTransaction(progressId, tx, 5, 5);
 * 		}
 * 		if (tx != null){
 * 			...
 * 			if (succeeded){
 * 				progressTracker.finishTransaction(progressId, processorId, tx.getTransactionId());
 * 			}else{
 * 				progressTracker.abortTransaction(progressId, processorId, tx.getTransactionId());
 * 			}
 * 		}
 * }
 * 
 * </pre>
 * 
 * @author James Hu
 *
 */
public interface TransactionalProgress {
	
	/**
	 * Try to start a transaction by picking up a previously failed transaction to retry. 
	 * <ul>
	 * 	<li>If the number of currently in progress transactions is less than maxConcurrentTransacions, then
	 * 		<ul>
	 * 			<li>If the number of currently retrying previously failed transactions is less than maxRetryingTransactions, 
	 * 				and there is at least one previously failed transaction needs retry, then start and return a previously failed transaction for retry</li>
	 * 			<li>Otherwise return a new transaction skeleton, with the startPosition set to the endPosition of the last transaction disregarding the
	 * 				state of the last transaction</li>
	 * 		</ul>
	 * 	<li>Otherwise return null.
	 * </ul>
	 * The returned object can be safely modified by the caller and reused for other purposes.
	 * @param progressId	ID of the progress
	 * @param processorId	ID of the processor which must currently own the transaction
	 * @param timeout		The time that the transaction (if started) will time out
	 * @param maxInProgressTransacions	maximum number of concurrent transaction
	 * @param maxRetryingTransactions	maximum number of retrying transactions
	 * @return Full information (transactionId is not null, startTime is not null) of a previously failed transaction just re-started, 
	 * 			or a skeleton (transactionId is the ID of the last transaction, startTime is null, endPosition is null) of a new transaction with startPosition specified 
	 * 			(the value will be the endPosition of the last transaction, can be null if this is the very first transaction),
	 * 			or null if no more concurrent transaction is allowed.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 */
	ProgressTransaction startTransaction(String progressId, String processorId, Instant timeout, int maxInProgressTransacions, int maxRetryingTransactions)
					throws InfrastructureErrorException;
	
	/**
	 * Try to start a transaction by picking up a previously failed transaction to retry. 
	 * <ul>
	 * 	<li>If the number of currently in progress transactions is less than maxConcurrentTransacions, then
	 * 		<ul>
	 * 			<li>If the number of currently retrying previously failed transactions is less than maxRetryingTransactions, 
	 * 				and there is at least one previously failed transaction needs retry, then start and return a previously failed transaction for retry</li>
	 * 			<li>Otherwise return a new transaction skeleton, with the startPosition set to the endPosition of the last transaction disregarding the
	 * 				state of the last transaction</li>
	 * 		</ul>
	 * 	<li>Otherwise return null.
	 * </ul>
	 * The returned object can be safely modified by the caller and reused for other purposes.
	 * @param progressId	ID of the progress
	 * @param processorId	ID of the processor which must currently own the transaction
	 * @param timeoutDuration		The duration after which the transaction (if started) will time out
	 * @param maxInProgressTransacions	maximum number of concurrent transaction
	 * @param maxRetryingTransactions	maximum number of retrying transactions
	 * @return Full information (transactionId is not null, startTime is not null) of a previously failed transaction just re-started, 
	 * 			or a skeleton (transactionId is the ID of the last transaction, startTime is null, endPosition is null) of a new transaction with startPosition specified 
	 * 			(the value will be the endPosition of the last transaction, can be null if this is the very first transaction),
	 * 			or null if no more concurrent transaction is allowed.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 */
	default ProgressTransaction startTransaction(String progressId, String processorId, Duration timeoutDuration, int maxInProgressTransacions, int maxRetryingTransactions)
			throws InfrastructureErrorException{
		return startTransaction(progressId, processorId, Instant.now().plus(timeoutDuration), maxInProgressTransacions, maxRetryingTransactions);
	}

	/**
	 * Try to start a new transaction.
	 * <ul>
	 * 	<li>If the number of currently in progress transactions is less than maxConcurrentTransacions, then
	 * 		<ul>
	 * 			<li>If the number of currently retrying previously failed transactions is less than maxRetryingTransactions, 
	 * 				and there is at least one previously failed transaction needs retry, then start and return a previously failed transaction for retry</li>
	 * 			<li>If the transaction specified in the arguments does not conflict with any existing transaction, then start and return the transaction specified. </li>
	 * 			<li>Otherwise return a new transaction skeleton, with the startPosition set to the endPosition of the last transaction disregarding the
	 * 				state of the last transaction</li>
	 * 		</ul>
	 * 	<li>Otherwise return null.
	 * </ul>
	 * The returned object can be safely modified by the caller and reused for other purposes.
	 * @param progressId			ID of the progress
	 * @param previousTransactionId	ID of previous transaction that the requested one must follow
	 * @param transaction		Details of this new transaction.
	 * 							<br>Fields that cannot be null are:
	 * 								processorId, startPosition, endPosition, timeout
	 * @param maxInProgressTransacions	maximum number of concurrent transaction
	 * @param maxRetryingTransactions	maximum number of retrying transactions
	 * @return Full information (transactionId is not null, startTime is not null) of a transaction 
	 * 			(can be the new transaction as specified by the argument, or a previously failed transaction) just started, 
	 * 			or a skeleton (transactionId is null, startTime is null, endPosition is null) of another new transaction with startPosition specified 
	 * 			(the value will be the endPosition of the last transaction, can be null if this is the very first transaction),
	 * 			or null if no more concurrent transaction is allowed.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 * @throws DuplicatedTransactionIdException if the transaction ID specified is duplicated
	 */
	ProgressTransaction startTransaction(String progressId, String previousTransactionId, ReadOnlyProgressTransaction transaction, int maxInProgressTransacions, int maxRetryingTransactions) 
					throws InfrastructureErrorException, DuplicatedTransactionIdException;
	
	/**
	 * Finish a succeeded transaction.
	 * @param progressId			ID of the progress
	 * @param processorId			ID of the processor which must currently own the transaction
	 * @param transactionId			ID of the transaction
	 * @throws NotOwningTransactionException 
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 * @throws IllegalTransactionStateException 
	 * @throws NoSuchTransactionException
	 */
	void finishTransaction(String progressId, String processorId, String transactionId) 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException;

	
	/**
	 * Abort a transaction.
	 * @param progressId			ID of the progress
	 * @param processorId			ID of the processor which must currently own the transaction
	 * @param transactionId			ID of the transaction
	 * @throws NotOwningTransactionException 
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 * @throws IllegalTransactionStateException 
	 */
	void abortTransaction(String progressId, String processorId, String transactionId) 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException;
	
	/**
	 * Update the time out of a transaction
	 * @param progressId			ID of the progress
	 * @param processorId			ID of the processor which must currently own the transaction
	 * @param transactionId			ID of the transaction
	 * @param timeout				The new time that the transaction should time out
	 * @throws NotOwningTransactionException 
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 * @throws NoSuchTransactionException
	 */
	void renewTransactionTimeout(String progressId, String processorId, String transactionId, Instant timeout) 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException;
	
	/**
	 * Update the time out of a transaction
	 * @param progressId			ID of the progress
	 * @param processorId			ID of the processor which must currently own the transaction
	 * @param transactionId			ID of the transaction
	 * @param timeout				The period from now after which the transaction should time out
	 * @throws NotOwningTransactionException 
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 * @throws IllegalTransactionStateException 
	 * @throws NoSuchTransactionException
	 */
	default void renewTransactionTimeout(String progressId, String processorId, String transactionId, Duration timeout) 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException{
		renewTransactionTimeout(progressId, processorId, transactionId, Instant.now().plus(timeout));
	}
	
	/**
	 * Check if a transaction had succeeded before a specific time
	 * @param progressId			ID of the progress
	 * @param transactionId			ID of the transaction
	 * @param beforeWhen			The time before which we want to know whether the transaction had succeeded or not 
	 * @return	true if the transaction had succeeded in the past or does not exist. false if the transaction is in-progress, aborted, or just succeeded a extremely short while ago.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 */
	boolean isTransactionSuccessful(String progressId, String transactionId, Instant beforeWhen) throws InfrastructureErrorException;
	
	/**
	 * Check if a transaction has succeeded
	 * @param progressId			ID of the progress
	 * @param transactionId			ID of the transaction
	 * @return	true if the transaction has succeeded in the past or does not exist. false if the transaction is in-progress, aborted, or just succeeded a extremely short while ago.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 */
	default boolean isTransactionSuccessful(String progressId, String transactionId) throws InfrastructureErrorException{
		return isTransactionSuccessful(progressId, transactionId, Instant.now());
	}

	/**
	 * Get recent transactions of a progress. 
	 * @param progressId	ID of the progress
	 * @return				List of recent transactions, can be empty if there is no transaction at all. 
	 * 						Head of the list is the last succeeded transaction, tail of the list is the most recent transaction.
	 * @throws InfrastructureErrorException if error in the underlying infrastructure happened
	 */
	List<ReadOnlyProgressTransaction> getRecentTransactions(String progressId) throws InfrastructureErrorException;

	/**
	 * Clear all the transactions of a progress. This method is not thread safe and should only be used for maintenance.
	 * @param progressId	ID of the progress
	 * @throws InfrastructureErrorException	if error in the underlying infrastructure happened
	 */
	void clear(String progressId) throws InfrastructureErrorException;

	/**
	 * Clear all the transactions of all progresses. This method is not thread safe and should only be used for maintenance.
	 * @param progressId	ID of the progress
	 * @throws InfrastructureErrorException	if error in the underlying infrastructure happened
	 */
	void clearAll() throws InfrastructureErrorException;
}
