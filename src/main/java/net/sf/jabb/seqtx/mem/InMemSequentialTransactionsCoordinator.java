/**
 * 
 */
package net.sf.jabb.seqtx.mem;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.util.col.PutIfAbsentMap;

import org.apache.commons.lang3.Validate;

/**
 * The implementation of SequentialTransactionsCoordinator that keeps all data in memory.
 * This implementation is intended for testing, PoC, and demo usage.
 * @author James Hu
 *
 */
public class InMemSequentialTransactionsCoordinator implements SequentialTransactionsCoordinator {
	
	protected Map<String, LinkedList<SimpleSequentialTransaction>> transactionsByseriesId;
	
	public InMemSequentialTransactionsCoordinator(){
		transactionsByseriesId = new PutIfAbsentMap<String, LinkedList<SimpleSequentialTransaction>>(new HashMap<String, LinkedList<SimpleSequentialTransaction>>(), k->new LinkedList<>());
	}

	/**
	 * Remove succeeded from the head and leave only one, transit those timed out to TIMED_OUT state,
	 * and remove the last transaction if it is a failed one with a null end position.
	 * @param transactions	 the list of transactions
	 */
	void compact(LinkedList<? extends SimpleSequentialTransaction> transactions){
		// remove finished historical transactions and leave only one of them
		int finished = 0;
		Iterator<? extends SimpleSequentialTransaction> iterator = transactions.iterator();
		if (iterator.hasNext()){
			if (iterator.next().isFinished()){
				finished ++;
				while(iterator.hasNext()){
					if (iterator.next().isFinished()){
						finished ++;
					}else{
						break;
					}
				}
			}
		}
		while (finished -- > 1){
			transactions.removeFirst();
		}
		
		// handle time out
		Instant now = Instant.now();
		for (SimpleSequentialTransaction tx: transactions){
			if (tx.isInProgress() && tx.getTimeout().isBefore(now)){
				if (!tx.timeout()){
					throw new IllegalStateException("Transaction '" + tx.getTransactionId() + "' is currently in " + tx.getState() + " state and cannot be changed to TIMED_OUT state");
				}
			}
		}
		
		// if the last transaction is failed and is open, remove it
		if (transactions.size() > 0){
			SimpleSequentialTransaction tx = transactions.getLast();
			if (tx.isFailed() && tx.getEndPosition() == null){
				transactions.removeLast();
			}
		}
	}
	
	static class TransactionCounts{
		int retryingCount;
		int inProgressCount;
		int failedCount;
	}
	
	private TransactionCounts getCounts(LinkedList<SimpleSequentialTransaction> transactions){
		TransactionCounts counts = new TransactionCounts();
		
		for (SimpleSequentialTransaction tx: transactions){
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
	
	TransactionCounts compactAndGetCounts(LinkedList<SimpleSequentialTransaction> transactions){
		compact(transactions);
		return getCounts(transactions);
	}


	@Override
	public SequentialTransaction startTransaction(String seriesId, String previousTransactionId,
			ReadOnlySequentialTransaction transaction, int maxInProgressTransacions,
			int maxRetryingTransactions) throws InfrastructureErrorException, DuplicatedTransactionIdException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transaction.getProcessorId(), "Processor ID cannot be null");
		Validate.notNull(transaction.getTimeout(), "Transaction time out cannot be null");
		if (transaction.getStartPosition() == null){	// startPosition is not null when restarting a specific transaction
			Validate.isTrue(null == transaction.getEndPosition(), "End position must be null when start position is null");
		}
		Validate.isTrue(maxInProgressTransacions > 0, "Maximum number of in-progress transactions must be greater than zero: %d", maxInProgressTransacions);
		Validate.isTrue(maxRetryingTransactions > 0, "Maximum number of retrying transactions must be greater than zero: %d", maxRetryingTransactions);
		Validate.isTrue(maxInProgressTransacions >= maxRetryingTransactions, "Maximum number of in-progress transactions must not be less than the maximum number of retrying transactions: %d, %d", maxInProgressTransacions, maxRetryingTransactions);

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		synchronized(transactions){
			TransactionCounts counts = compactAndGetCounts(transactions);
			
			if (counts.inProgressCount >= maxInProgressTransacions ||  // no more transaction allowed
					counts.inProgressCount > 0 && transactions.getLast().getEndPosition() == null && transactions.getLast().isInProgress()  // the last one is in-progress and is open
					){	
				return null;
			}
			
			if (counts.retryingCount < maxRetryingTransactions && counts.failedCount > 0){	// always first try to pick up a failed to retry
				Optional<SimpleSequentialTransaction> firstFailed = transactions.stream().filter(tx->tx.isFailed()).findFirst();
				if (firstFailed.isPresent()){
					SimpleSequentialTransaction tx = firstFailed.get();
					if (!tx.retry(transaction.getProcessorId(), transaction.getTimeout())){
						throw new IllegalStateException("Cann't retry transaction: " +  tx);
					}
					return SimpleSequentialTransaction.copyOf(tx);
				}
			}
			
			SimpleSequentialTransaction tx;
			if (transaction.getStartPosition() == null){		// the client has nothing in mind, so propose a new one
				if (transactions.size() > 0){
					SimpleSequentialTransaction last = transactions.getLast();
					tx = new SimpleSequentialTransaction(last.getTransactionId(), transaction.getProcessorId(), last.getEndPosition(), transaction.getTimeout());
				}else{
					tx = new SimpleSequentialTransaction(null, transaction.getProcessorId(), null, transaction.getTimeout());
				}
			}else{		// try to start the transaction requested by the client
				if ( transactions.size() == 0 || transactions.getLast().getTransactionId().equals(previousTransactionId)){
					// start the requested one
					SimpleSequentialTransaction newTrans = SimpleSequentialTransaction.copyOf(transaction);
					newTrans.setAttempts(1);
					newTrans.setStartTime(Instant.now());
					newTrans.setFinishTime(null);
					newTrans.setState(SequentialTransactionState.IN_PROGRESS);
					String transactionId = newTrans.getTransactionId();
					if (transactionId == null){
						newTrans.setTransactionId(UUID.randomUUID().toString());
					}else{
						Validate.notBlank(transactionId, "Transaction ID cannot be blank: %s", transactionId);
						if (transactions.stream().anyMatch(t->t.getTransactionId().equals(transactionId))){
							throw new DuplicatedTransactionIdException("Transaction ID '" + transactionId + "' is duplicated");
						}
					}
					transactions.addLast(newTrans);
					tx = SimpleSequentialTransaction.copyOf(newTrans);
				}else{
					// propose a new one
					SimpleSequentialTransaction last = transactions.getLast();
					tx = new SimpleSequentialTransaction(last.getTransactionId(), transaction.getProcessorId(), last.getEndPosition(), transaction.getTimeout());
				}
			}
			
			return tx;
		}
	}

	@Override
	public void finishTransaction(String seriesId, String processorId,
			String transactionId, String endPosition) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		synchronized(transactions){
			compact(transactions);

			Optional<SimpleSequentialTransaction> matched = transactions.stream().filter(tx->tx.getTransactionId().equals(transactionId)).findFirst();
			if (matched.isPresent()){
				SimpleSequentialTransaction tx = matched.get();
				if (tx.getProcessorId().equals(processorId)){
					String updatedEndPosition = tx.getEndPosition();
					if (endPosition != null){
						if (tx == transactions.getLast()){
							updatedEndPosition = endPosition;
						}else{
							if (!endPosition.equals(tx.getEndPosition())){
								// can't change the end position of a non-last transaction
								throw new IllegalEndPositionException("Cannot change end position of transaction '" + transactionId + "' from '" + tx.getEndPosition() + "' to '" + endPosition + "' because it is not the last transaction");
							}
						}
					}
					if (updatedEndPosition == null){
						// cannot finish an open transaction
						throw new IllegalEndPositionException("Cannot finish transaction '" + transactionId + "' with a null end position");
					}
					if (tx.finish()){
						tx.setEndPosition(updatedEndPosition);
						compact(transactions);
					}else{
						throw new IllegalTransactionStateException("Transaction '" + transactionId + "' is currently in " + tx.getState() + " state and cannot be changed to FINISHED state");
					}
				}else{
					throw new NotOwningTransactionException("Transaction '" + transactionId + "' is currently owned by processor '" + tx.getProcessorId() + "', not '" + processorId + "'");
				}
			}else{
				throw new NoSuchTransactionException("Transaction '" + transactionId + "' either does not exist or have succeeded and then been purged");
			}
		}
	}

	@Override
	public void abortTransaction(String seriesId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		synchronized(transactions){
			compact(transactions);

			Optional<SimpleSequentialTransaction> matched = transactions.stream().filter(tx->tx.getTransactionId().equals(transactionId)).findAny();
			if (matched.isPresent()){
				SimpleSequentialTransaction tx = matched.get();
				if (tx.getProcessorId().equals(processorId)){
					if (!tx.abort()){
						throw new IllegalTransactionStateException("Transaction '" + transactionId + "' is currently in " + tx.getState() + " state and cannot be changed to ABORTED state");
					}
					compact(transactions);
				}else{
					throw new NotOwningTransactionException("Transaction '" + transactionId + "' is currently owned by processor '" + tx.getProcessorId() + "', not '" + processorId + "'");
				}
			}else{
				throw new NoSuchTransactionException("Transaction '" + transactionId + "' either does not exist or have succeeded and then been purged");
			}
		}
	}

	@Override
	public List<? extends ReadOnlySequentialTransaction> getRecentTransactions(String seriesId)
			throws InfrastructureErrorException {
		Validate.notNull(seriesId, "Series ID cannot be null");

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		LinkedList<SimpleSequentialTransaction> copy = new LinkedList<>();
		synchronized(transactions){
			compact(transactions);
			for (SimpleSequentialTransaction tx: transactions){
				copy.add(SimpleSequentialTransaction.copyOf(tx));
			}
		}
		compact(copy);
		return copy;
	}
	
	@Override
	public boolean isTransactionSuccessful(String seriesId, String transactionId, Instant beforeWhen) {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");
		Validate.notNull(beforeWhen, "Time cannot be null");

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		synchronized(transactions){
			compact(transactions);
			
			if (transactions.size() > 0){
				SimpleSequentialTransaction first = transactions.getFirst(); // the last known successful if exists
				if (first.isFinished() && beforeWhen.isBefore(first.getFinishTime())){
					return true;
				}
			}
			
			Optional<SimpleSequentialTransaction> matched = transactions.stream().filter(tx->tx.getTransactionId().equals(transactionId)).findAny();
			if (matched.isPresent()){
				return matched.get().isFinished();
			}else{
				return true; // id does not match either last succeeded or current, so the transaction must have succeeded and later been purged
			}
		}
	}
	
	@Override
	public void renewTransactionTimeout(String seriesId, String processorId, String transactionId, Instant transactionTimeout) 
			throws NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionTimeout, "Transaction time out cannot be null");

		LinkedList<SimpleSequentialTransaction> transactions = transactionsByseriesId.get(seriesId);
		synchronized(transactions){
			compact(transactions);
			
			Optional<SimpleSequentialTransaction> matched = transactions.stream().filter(tx->tx.getTransactionId().equals(transactionId)).findAny();
			if (matched.isPresent()){
				SimpleSequentialTransaction tx = matched.get();
				if (tx.getProcessorId().equals(processorId)){
					if (tx.isInProgress()){
						tx.setTimeout(transactionTimeout);
					}else{
						throw new IllegalTransactionStateException("Transaction '" + transactionId + "' is currently in " + tx.getState() + " state and its timeout cannot be changed");
					}
				}else{
					throw new NotOwningTransactionException("Transaction '" + transactionId + "' is currently owned by processor '" + tx.getProcessorId() + "', not '" + processorId + "'");
				}
			}else{
				throw new NoSuchTransactionException("Transaction '" + transactionId + "' either does not exist or have succeeded and then been purged");
			}
		}
	}

	@Override
	public void clear(String seriesId) throws InfrastructureErrorException {
		Validate.notNull(seriesId, "Series ID cannot be null");

		this.transactionsByseriesId.remove(seriesId);
	}

	@Override
	public void clearAll() throws InfrastructureErrorException {
		this.transactionsByseriesId.clear();
	}



}