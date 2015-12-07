/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.ConsumerThrowsExceptions;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.PredicateThrowsExceptions;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.text.DurationFormatter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableRequestOptions;

/**
 * The implementation of SequentialTransactionsCoordinator that is backed by Microsoft Azure table storage.
 * The existence of the underlying table is checked and ensured only once during the life time of the instance of this class.
 * @author James Hu
 *
 */
public class AzureSequentialTransactionsCoordinator implements SequentialTransactionsCoordinator {
	static private final Logger logger = LoggerFactory.getLogger(AzureSequentialTransactionsCoordinator.class);

	static protected final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();

	/**
	 * The default attempt strategy for Azure operations, with maximum 90 seconds allowed in total, and 0.5 to 10 seconds backoff interval 
	 * according to fibonacci series.
	 */
	static public final AttemptStrategy DEFAULT_ATTEMPT_STRATEGY = new AttemptStrategy()
		.withWaitStrategy(WaitStrategies.threadSleepStrategy())
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(90)))
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(500L, 1000L * 10));
	
	public static final String DEFAULT_TABLE_NAME = "SequentialTransactionsCoordinator";
	public static final String DUMMY_FIRST_TRANSACTION_ID = "DUMMY_FIRST_TRANSACTION_ID" + "|||||||";
	
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile SimpleSequentialTransaction lastSucceededTransactionCached;
	protected volatile boolean tableExists = false;
	
	protected AttemptStrategy attemptStrategy = DEFAULT_ATTEMPT_STRATEGY;
	
	protected static final Predicate<Exception> ENTITY_HAS_BEEN_MODIFIED_BY_OTHERS = AzureStorageUtility::isUpdateConditionNotSatisfied;
	
	protected static final Predicate<Exception> ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS = AzureStorageUtility::isNotFoundOrUpdateConditionNotSatisfied;
	
	public AzureSequentialTransactionsCoordinator(){
		
	}
	
	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, String tableName, AttemptStrategy attemptStrategy, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		if (attemptStrategy != null){
			this.attemptStrategy = attemptStrategy;
		}
		tableClient = storageAccount.createCloudTableClient();
		if (defaultOptionsConfigurer != null){
			defaultOptionsConfigurer.accept(tableClient.getDefaultRequestOptions());
		}
	}
	
	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, String tableName, AttemptStrategy attemptStrategy){
		this(storageAccount, tableName, attemptStrategy, null);
	}

	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, String tableName){
		this(storageAccount, tableName, null, null);
	}

	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this(storageAccount, null, null, defaultOptionsConfigurer);
	}

	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount){
		this(storageAccount, null, null, null);
	}
	
	public AzureSequentialTransactionsCoordinator(CloudTableClient tableClient, String tableName, AttemptStrategy attemptStrategy){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		this.tableClient = tableClient;
		if (attemptStrategy != null){
			this.attemptStrategy = attemptStrategy;
		}
	}

	public AzureSequentialTransactionsCoordinator(CloudTableClient tableClient, AttemptStrategy attemptStrategy){
		this(tableClient, null, attemptStrategy);
	}

	public AzureSequentialTransactionsCoordinator(CloudTableClient tableClient){
		this(tableClient, null, null);
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
	}

	public void setTableClient(AttemptStrategy attemptStrategy) {
		this.attemptStrategy = attemptStrategy;
	}
	
	/**
	 * Generate a 22-character presented random UUID
	 * @return base64 URL safe encoded UUID
	 */
	protected String newUniqueTransactionId(){
		UUID uuid = UUID.randomUUID();
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		
		String encoded = base64.encodeToString(bb.array());
		return encoded;
	}
	
	@Override
	public SequentialTransaction startTransaction(String seriesId,
			String previousTransactionId, String previousTransactionEndPosition,
			ReadOnlySequentialTransaction transaction,
			int maxInProgressTransacions, int maxRetryingTransactions)
			throws TransactionStorageInfrastructureException,
			DuplicatedTransactionIdException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transaction.getProcessorId(), "Processor ID cannot be null");
		Validate.notNull(transaction.getTimeout(), "Transaction time out cannot be null");
		if (transaction.getStartPosition() == null){	// startPosition is not null when restarting a specific transaction
			Validate.isTrue(null == transaction.getEndPosition(), "End position must be null when start position is null");
		}
		if (previousTransactionId != null){
			Validate.notNull(previousTransactionEndPosition, "previousTransactionEndPosition cannot be null when previousTransactionId is not null: " + previousTransactionId);
		}
		Validate.isTrue(maxInProgressTransacions > 0, "Maximum number of in-progress transactions must be greater than zero: %d", maxInProgressTransacions);
		Validate.isTrue(maxRetryingTransactions > 0, "Maximum number of retrying transactions must be greater than zero: %d", maxRetryingTransactions);
		Validate.isTrue(maxInProgressTransacions >= maxRetryingTransactions, "Maximum number of in-progress transactions must not be less than the maximum number of retrying transactions: %d, %d", maxInProgressTransacions, maxRetryingTransactions);

		long startTime = System.currentTimeMillis();
		// try to pick up a failed one for retrying
		SequentialTransaction retryingTransaction = startAnyFailedTransaction(seriesId, transaction.getProcessorId(), transaction.getTimeout(), maxInProgressTransacions, maxRetryingTransactions);
		if (retryingTransaction != null){
			return retryingTransaction;
		}
		
		long finishStartAnyFailedTime = System.currentTimeMillis();
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		if (transactions.size() > 0 && StringUtils.isNotEmpty(previousTransactionEndPosition)){
			Validate.notNull(previousTransactionId, "previousTransactionId cannot be null when previousTransactionEndPosition has a value");
		}
		TransactionCounts counts = SequentialTransactionsCoordinator.getTransactionCounts(transactions);
		ReadOnlySequentialTransaction last = transactions.size() > 0 ? transactions.get(transactions.size() - 1) : null;
		long finishGetRecentTime = System.currentTimeMillis();

		if (counts.getInProgress() >= maxInProgressTransacions){  // no more transaction allowed
			return null;
		}
		
		if (counts.getInProgress() > 0 && last.getEndPosition() == null && last.isInProgress()){  // the last one is in-progress and is open
			return null;
		}
		
		if (transaction.getStartPosition() == null){		// the client has nothing in mind, so propose a new one
			return newNextTransaction(last, transaction.getProcessorId(), transaction.getTimeout());
		}else{		// try to start the transaction requested by the client
			if (last == null || last.getTransactionId().equals(previousTransactionId)
					|| previousTransactionId == null && DUMMY_FIRST_TRANSACTION_ID.equals(last.getTransactionId())  ){
				// start the requested one
				SimpleSequentialTransaction newTrans = SimpleSequentialTransaction.copyOf(transaction);
				newTrans.setAttempts(1);
				newTrans.setStartTime(Instant.now());
				newTrans.setFinishTime(null);
				newTrans.setState(SequentialTransactionState.IN_PROGRESS);
				String transactionId = newTrans.getTransactionId();
				if (transactionId == null){
					newTrans.setTransactionId(newUniqueTransactionId());
				}else{
					Validate.notBlank(transactionId, "Transaction ID cannot be blank: %s", transactionId);
					if (transactions.stream().anyMatch(t->t.getTransactionId().equals(transactionId))){
						throw new DuplicatedTransactionIdException("Transaction ID '" + transactionId + "' is duplicated");
					}
				}
				try {
					SequentialTransactionEntity createdEntity = createNewTransaction(seriesId, previousTransactionId, previousTransactionEndPosition, newTrans);
					logger.debug("Created new transaction '{}' after '{}'. tryStartAnyFailed: {}, getRecent: {}, createNew(succeeded): {}", 
							createdEntity.keysToString(), previousTransactionId,
							DurationFormatter.format(finishStartAnyFailedTime-startTime), 
							DurationFormatter.format(finishGetRecentTime-finishStartAnyFailedTime),
							DurationFormatter.formatSince(finishGetRecentTime));
					return createdEntity.toSequentialTransaction();
				} catch (IllegalStateException e) {	// the last one is no longer the last
					logger.debug("Transaction '{}/{}' is no longer the last. tryStartAnyFailed: {}, getRecent: {}, createNew(failed): {}", 
							seriesId, previousTransactionId,
							DurationFormatter.format(finishStartAnyFailedTime-startTime), 
							DurationFormatter.format(finishGetRecentTime-finishStartAnyFailedTime),
							DurationFormatter.formatSince(finishGetRecentTime));
					SequentialTransactionEntity latestLast = null;
					try{
						latestLast = fetchLastTransactionEntity(seriesId);
						return newNextTransactionOrNull(latestLast, transaction.getProcessorId(), transaction.getTimeout());
					}catch(Exception e1){
						throw new TransactionStorageInfrastructureException("Failed to fetch latest last transaction in series '" + seriesId + "'", e1);
					}
				} catch (StorageException e){
					throw new TransactionStorageInfrastructureException("Failed to create after the last one with ID '" + previousTransactionId + "' a new transaction: " + newTrans, e);
				}
			}else{
				// propose a new one
				return newNextTransaction(last, transaction.getProcessorId(), transaction.getTimeout());
			}
		}
	}
	
	/**
	 * Create an instance of SequentialTransaction that is the next to a specified one, or return null if it is not possible
	 * @param previous			the previous transaction which should be the last in the series
	 * @param processorId	ID of the processor
	 * @param timeout		time out of the to be created transaction
	 * @return		a new transaction, or null if previous is null or has a null end position
	 */
	protected SequentialTransaction newNextTransactionOrNull(ReadOnlySequentialTransaction previous, String processorId, Instant timeout){
		if (previous == null || previous.getEndPosition() == null){
			return null;
		}else if (DUMMY_FIRST_TRANSACTION_ID.equals(previous.getTransactionId())){
			return new SimpleSequentialTransaction(null, processorId, null, timeout);
		}else{
			return new SimpleSequentialTransaction(previous.getTransactionId(), processorId, previous.getEndPosition(), timeout);
		}
	}
	
	/**
	 * Create an instance of SequentialTransaction that is the next to a specified one
	 * @param previous			the previous transaction which should be the last in the series
	 * @param processorId	ID of the processor
	 * @param timeout		time out of the to be created transaction
	 * @return		a new transaction
	 */
	protected SequentialTransaction newNextTransaction(ReadOnlySequentialTransaction previous, String processorId, Instant timeout){
		if (previous == null || previous.getEndPosition() == null || 
				DUMMY_FIRST_TRANSACTION_ID.equals(previous.getTransactionId())){
			return new SimpleSequentialTransaction(null, processorId, null, timeout);
		}else {
			return new SimpleSequentialTransaction(previous.getTransactionId(), processorId, previous.getEndPosition(), timeout);
		}
	}
	
	/**
	 * Create a new transaction after the last one
	 * @param seriesId					ID of the series
	 * @param lastTransactionId			ID of the last transaction
	 * @param previousTransactionEndPosition	End position of the last transaction 
	 * 									- if the current value does not match this value, the new transaction should not be created
	 * @param newTrans					the new transaction
	 * @return the transaction entity created
	 * @throws IllegalStateException	if the last transaction is no longer the last
	 * @throws StorageException			any error happened when updating the last and inserting the new one
	 * @throws TransactionStorageInfrastructureException		if unable to get table reference
	 */
	protected SequentialTransactionEntity createNewTransaction(String seriesId, String lastTransactionId, String previousTransactionEndPosition, SimpleSequentialTransaction newTrans) throws IllegalStateException, StorageException, TransactionStorageInfrastructureException{
		CloudTable table = getTableReference();
		SequentialTransactionEntity last = null;
		if (lastTransactionId == null){ // the first one
			last = fetchEntity(seriesId, DUMMY_FIRST_TRANSACTION_ID);
			if (last == null){  // the actual first
				// we must create a dummy last one for concurrency control
				last = new SequentialTransactionEntity();
				last.setSeriesId(seriesId);
				last.setTransactionId(DUMMY_FIRST_TRANSACTION_ID);
				last.setFirstTransaction();
				last.setLastTransaction();
				last.setState(SequentialTransactionState.FINISHED);
				last.setStartTime(Instant.ofEpochMilli(0));
				last.setFinishTime(Instant.ofEpochMilli(0));
				try{
					table.execute(TableOperation.insert(last));
				}catch(StorageException e){
					if (e.getHttpStatusCode() == 409 && StorageErrorCodeStrings.ENTITY_ALREADY_EXISTS.equals(e.getErrorCode())){	// someone is faster
						throw new IllegalStateException("A new transaction is now the last one");
					}else{
						throw e;
					}
				}
			}else{  // previously a first transaction aborted, left the dummy first one there
				if (!last.isLastTransaction()){
					throw new IllegalStateException("The transaction in series '" + seriesId + "' is no longer the last one: " + lastTransactionId);
				}
			}
		}else{
			last = fetchEntity(seriesId, lastTransactionId);
			if (last == null || !last.isLastTransaction()){
				throw new IllegalStateException("The transaction in series '" + seriesId + "' is no longer the last one: " + lastTransactionId);
			}
			if (!StringUtils.equals(previousTransactionEndPosition, last.getEndPosition())){
				throw new IllegalStateException("The transaction in series '" + seriesId + "' has changed its end position from '" 
						+  previousTransactionEndPosition + "' to '" + last.getEndPosition() + "': " + lastTransactionId);
			}
		}
		SequentialTransactionEntity next = SequentialTransactionEntity.fromSequentialTransaction(seriesId, newTrans, last.getTransactionId(), null);
		last.setNextTransactionId(next.getTransactionId());
		next.setPreviousTransactionId(last.getTransactionId());
		next.setLastTransaction();
		
		// do in a transaction: update the last, and insert the new one
		TableBatchOperation batchOperation = new TableBatchOperation();
		batchOperation.add(TableOperation.merge(last));
		batchOperation.add(TableOperation.insert(next));
		try{
			table.execute(batchOperation);
		}catch(StorageException e){
			if (ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS.test(e)){
				throw new IllegalStateException("The transaction is no longer the last one: " + last.keysToString());
			}else{
				throw e;
			}
		}
		
		return next;
		// TODO: check the duplicated keys exception
	}
	
	@Override
	public SequentialTransaction startAnyFailedTransaction(String seriesId, String processorId, Instant timeout, int maxInProgressTransacions, int maxRetryingTransactions)
			throws TransactionStorageInfrastructureException{
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(timeout, "Transaction time out cannot be null");
		Validate.isTrue(maxInProgressTransacions > 0, "Maximum number of in-progress transactions must be greater than zero: %d", maxInProgressTransacions);
		Validate.isTrue(maxRetryingTransactions > 0, "Maximum number of retrying transactions must be greater than zero: %d", maxRetryingTransactions);
		Validate.isTrue(maxInProgressTransacions >= maxRetryingTransactions, "Maximum number of in-progress transactions must not be less than the maximum number of retrying transactions: %d, %d", maxInProgressTransacions, maxRetryingTransactions);

		try{
			return new AttemptStrategy(attemptStrategy)
				.overrideBackoffStrategy(BackoffStrategies.noBackoff())
				.retryIfException(IllegalTransactionStateException.class)
				.retryIfException(NoSuchTransactionException.class)
				.callThrowingSuppressed(()->{return doStartAnyFailedTransaction(seriesId, processorId, timeout, maxInProgressTransacions, maxRetryingTransactions);});
		}catch(Exception e){
			throw new TransactionStorageInfrastructureException("Failed to start failed transaction for retrying: " + seriesId, e);
		}
	}
	
	protected SequentialTransaction doStartAnyFailedTransaction(String seriesId, String processorId, Instant timeout, int maxInProgressTransacions, int maxRetryingTransactions)
			throws TransactionStorageInfrastructureException, IllegalTransactionStateException, NoSuchTransactionException, Exception{
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		TransactionCounts counts = SequentialTransactionsCoordinator.getTransactionCounts(transactions);

		if (counts.getInProgress() >= maxInProgressTransacions){  // no more transaction allowed
			return null;
		}
		
		if (counts.getRetrying() < maxRetryingTransactions && counts.getFailed() > 0){	// always first try to pick up a failed to retry
			List<? extends ReadOnlySequentialTransaction> allFailed = transactions.stream().filter(tx->tx.isFailed()).collect(Collectors.toList());
			for (ReadOnlySequentialTransaction failed: allFailed){
				ReadOnlySequentialTransaction tx = failed;
				String transactionKey = AzureStorageUtility.keysToString(seriesId, tx.getTransactionId());
				AtomicReference<SequentialTransaction> startedTx = new AtomicReference<>(null);
				try {
					new AttemptStrategy(attemptStrategy)
						.overrideBackoffStrategy(BackoffStrategies.noBackoff())
						.retryIfException(ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS)
						.runThrowingSuppressed(()->modifyTransaction(seriesId, null, tx.getTransactionId(), 
								entity->entity.retry(processorId, timeout), entity->{
									CloudTable table = getTableReference();
									table.execute(TableOperation.replace(entity));
									startedTx.set(entity.toSequentialTransaction());
								}));
					return startedTx.get();
				} catch (TransactionStorageInfrastructureException e){
					throw e;
				} catch (IllegalTransactionStateException | NoSuchTransactionException e){ // picked up by someone else
					continue; // try next one
				} catch (Exception e){	// only possible: StorageException|RuntimeException
					throw new TransactionStorageInfrastructureException("Failed to update transaction entity for retry: " + transactionKey, e);
				}
			}
		}
		return null;	// no transaction can be started for retrying
	}

	@Override
	public void finishTransaction(String seriesId, String processorId,
			String transactionId, String endPosition) throws NotOwningTransactionException,
			TransactionStorageInfrastructureException, IllegalTransactionStateException,
			NoSuchTransactionException, IllegalEndPositionException {
		//Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction ID cannot be null");

		try {
			AtomicReference<String> updatedEndPosition = new AtomicReference<>(null);
			new AttemptStrategy(attemptStrategy)
				.overrideBackoffStrategy(BackoffStrategies.noBackoff())
				.retryIfException(ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS)
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->{
							updatedEndPosition.set(entity.getEndPosition());
							if (endPosition != null){
								if (entity.isLastTransaction()){
									updatedEndPosition.set(endPosition);
								}else{
									if (!endPosition.equals(entity.getEndPosition())){
										// can't change the end position of a non-last transaction
										String transactionKey = AzureStorageUtility.keysToString(entity);
										throw new IllegalEndPositionException("Cannot change transaction end position from '" + entity.getEndPosition() + "' to '" + endPosition + "' because it is not the last transaction: " + transactionKey);
									}
								}
							}
							if (updatedEndPosition.get() == null){
								// cannot finish an open transaction
								String transactionKey = AzureStorageUtility.keysToString(entity);
								throw new IllegalEndPositionException("Cannot finish transaction with a null end position: " + transactionKey);
							}
							return entity.finish();
						}, entity->{
							entity.setEndPosition(updatedEndPosition.get());
							CloudTable table = getTableReference();
							try{
								table.execute(TableOperation.replace(entity));
							}catch(StorageException e){
								if (e.getHttpStatusCode() == 404){
									throw new IllegalTransactionStateException("Transaction may already have been timed out or finished and then have been deleted: " + entity.keysToString());
								}else{
									throw e;
								}
							}
						}));
		} catch (NotOwningTransactionException | TransactionStorageInfrastructureException | IllegalTransactionStateException | IllegalEndPositionException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new TransactionStorageInfrastructureException("Failed to update transaction entity state to " + SequentialTransactionState.FINISHED + ": " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
	}

	@Override
	public void abortTransaction(String seriesId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			TransactionStorageInfrastructureException, IllegalTransactionStateException,
			NoSuchTransactionException {
		//Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");

		try {
			new AttemptStrategy(attemptStrategy)
				.overrideBackoffStrategy(BackoffStrategies.noBackoff())
				.retryIfException(ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS)
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->entity.abort(), entity->{
							CloudTable table = getTableReference();
							table.execute(TableOperation.replace(entity));
						}));
		} catch (NotOwningTransactionException | TransactionStorageInfrastructureException | IllegalTransactionStateException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new TransactionStorageInfrastructureException("Failed to update transaction entity state to " + SequentialTransactionState.ABORTED + ": " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
	}

	@Override
	public void updateTransaction(String seriesId, String processorId,
			String transactionId, String endPosition, Instant timeout, Serializable detail)
			throws NotOwningTransactionException, TransactionStorageInfrastructureException,
			IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException {
		//Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.isTrue(endPosition != null || timeout != null || detail != null, "End position, time out, and detail cannot all be null");

		try {
			new AttemptStrategy(attemptStrategy)
				.overrideBackoffStrategy(BackoffStrategies.noBackoff())
				.retryIfException(ENTITY_HAS_BEEN_DELETED_OR_MODIFIED_BY_OTHERS)
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->entity.isInProgress(),
						entity->{
							if (endPosition != null){
								if (endPosition.equals(entity.getEndPosition())){
									// do nothing
								}else if (entity.isLastTransaction()){
									entity.setEndPosition(endPosition);
								}else{
									String transactionKey = AzureStorageUtility.keysToString(entity);
									throw new IllegalEndPositionException("Cannot change transaction end position from '" + entity.getEndPosition() + "' to '" + endPosition + "' because it is not the last transaction: " + transactionKey);
								}
							}
							if (timeout != null){
								entity.setTimeout(timeout);
							}
							if (detail != null){
								entity.setDetail(detail);
							}
							CloudTable table = getTableReference();
							table.execute(TableOperation.replace(entity));
						}));
		} catch (NotOwningTransactionException | TransactionStorageInfrastructureException | IllegalTransactionStateException | NoSuchTransactionException | IllegalEndPositionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new TransactionStorageInfrastructureException("Failed to update transaction entity with keys: " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
			
	}
	
	/**
	 * Perform modification of a transaction
	 * @param seriesId							ID of the series, can be null
	 * @param processorId						ID of the process that this transaction must belong to, or null if there is no need to check this
	 * @param transactionId						ID of the transaction
	 * @param stateChecker						lambda to check whether the transaction state is okay, returns true for ok false for throwing IllegalTransactionStateException
	 * @param updater							lambda to perform the update, StorageException is the only unchecked exception allowed to be thrown
	 * @throws NotOwningTransactionException		if the transaction is not currently owned by the process with specified processId
	 * @throws TransactionStorageInfrastructureException			if failed to update the entity
	 * @throws IllegalTransactionStateException		if the state of the transaction is not IN_PROGRESS
	 * @throws NoSuchTransactionException	if no such transaction can be found
	 * @throws StorageException				if failed to replace the entity with updated values
	 */
	protected void modifyTransaction(String seriesId, String processorId, String transactionId, 
			PredicateThrowsExceptions<SequentialTransactionEntity> stateChecker, ConsumerThrowsExceptions<SequentialTransactionEntity> updater)
			throws NotOwningTransactionException, TransactionStorageInfrastructureException,
			IllegalTransactionStateException, NoSuchTransactionException, StorageException {
		// update a single transaction
		String transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
		SequentialTransactionEntity entity = null;
		try {
			if (seriesId == null){
				entity = fetchEntity(transactionId);
				seriesId = entity.getPartitionKey();
				transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
			}else{
				entity = fetchEntity(seriesId, transactionId);
			}
		} catch (StorageException e) {
			throw new TransactionStorageInfrastructureException("Failed to fetch transaction entity with keys: " + transactionKey, e);
		}
		if (entity == null){
			throw new NoSuchTransactionException("Transaction either does not exist or have succeeded and later been purged: " + transactionKey);
		}
		if (processorId != null && !processorId.equals(entity.getProcessorId())){
			throw new NotOwningTransactionException("Transaction is currently owned by processor '" + entity.getProcessorId() + "', not '" + processorId + "': " + transactionKey);
		}
		
		if (ExceptionUncheckUtility.testThrowingUnchecked(stateChecker, entity)){
			ExceptionUncheckUtility.acceptThrowingUnchecked(updater, entity);
		}else{
			throw new IllegalTransactionStateException("Transaction is currently in " + entity.getState() + " state:" + transactionKey);
		}
	}

	@Override
	public boolean isTransactionSuccessful(String seriesId,	String transactionId)
			throws TransactionStorageInfrastructureException {
		//Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transactionId, "Transaction id cannot be null");

		// try cached first
		/*	this check is for current VM only
		if (lastSucceededTransactionCached != null){
			if (!beforeWhen.isAfter(lastSucceededTransactionCached.getFinishTime())){
				return true;
			}
		}
		*/
		SequentialTransactionEntity entity;
		try {
			entity = seriesId == null? fetchEntity(transactionId) : fetchEntity(seriesId, transactionId);
			return entity == null || entity.isFinished();
		} catch (StorageException e) {
			throw new TransactionStorageInfrastructureException("Failed to fetch transaction entity: " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
		/*
		// reload
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		
		if (lastSucceededTransactionCached != null){
			if (!beforeWhen.isAfter(lastSucceededTransactionCached.getFinishTime())){
				return true;
			}
		}

		// check in recent transactions
		Optional<? extends ReadOnlySequentialTransaction> matched = transactions.stream().filter(tx -> tx.getTransactionId().equals(transactionId)).findAny();
		if (matched.isPresent()){
			return matched.get().isFinished();
		}
		
		return true;		// must be a very old succeeded transaction
		*/
	}

	protected List<? extends ReadOnlySequentialTransaction> getRecentTransactionsIncludingDummy(
			String seriesId) throws TransactionStorageInfrastructureException {
		LinkedList<SequentialTransactionWrapper> transactionEntities = new LinkedList<>();
		//AtomicInteger attempts = new AtomicInteger(0);
		try{
			new AttemptStrategy(attemptStrategy)
			.overrideBackoffStrategy(BackoffStrategies.noBackoff())
			.retryIfResultEquals(Boolean.FALSE)		// retry until consistent but may be not up to date
			.callThrowingAll(()->{
				//attempts.incrementAndGet();
				// get entities by seriesId
				Map<String, SequentialTransactionWrapper> wrappedTransactionEntities = fetchEntities(seriesId, true);
				transactionEntities.clear();
				transactionEntities.addAll(toList(wrappedTransactionEntities));
				
				// compact the list
				return compact(transactionEntities);
			});
		}catch(TransactionStorageInfrastructureException e){
			throw e;
		}catch(Exception e){
			throw new TransactionStorageInfrastructureException("Failed to fetch recent transactions for series '" + seriesId + "'", e);
		}
		
		//logger.debug("Attempted {} times for getting a consistent snapshot of recent transactions in series: {}", attempts.get(), seriesId);
		return transactionEntities.stream().map(SequentialTransactionWrapper::getTransactionNotNull).collect(Collectors.toList());
	}

	@Override
	public List<? extends ReadOnlySequentialTransaction> getRecentTransactions(
			String seriesId) throws TransactionStorageInfrastructureException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		if (transactions.size() > 0){
			if(DUMMY_FIRST_TRANSACTION_ID.equals(transactions.get(0).getTransactionId())){
				transactions.remove(0);
			}
		}
		return transactions;
	}


	@Override
	public void clear(String seriesId) throws TransactionStorageInfrastructureException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		// delete entities by seriesId
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExists(table, 
					TableQuery.generateFilterCondition(
							AzureStorageUtility.PARTITION_KEY, 
							QueryComparisons.EQUAL,
							seriesId));
			logger.debug("Deleted all transactions in series '{}' in table: {}", seriesId, table.getName()); 
		}catch(Exception e){
			throw new TransactionStorageInfrastructureException("Failed to delete entities belonging to series '" + seriesId + "' in table: " + tableName, e);
		}
	}

	@Override
	public void clearAll() throws TransactionStorageInfrastructureException {
		// delete all entities
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExists(table, (String)null);
			logger.debug("Deleted all transactions in all series in table: {}", table.getName()); 
		}catch(Exception e){
			throw new TransactionStorageInfrastructureException("Failed to delete all entities in table: " + tableName, e);
		}
	}
	
	protected CloudTable getTableReference() throws TransactionStorageInfrastructureException{
		CloudTable table;
		try {
			table = tableClient.getTableReference(tableName);
		} catch (Exception e) {
			throw new TransactionStorageInfrastructureException("Failed to get reference for table: '" + tableName + "'", e);
		}
		if (!tableExists){
			try {
				if (AzureStorageUtility.createIfNotExists(tableClient, tableName)){
					logger.debug("Created table: {}", tableName); 
				}
			} catch (Exception e) {
				throw new TransactionStorageInfrastructureException("Failed to ensure the existence of table: '" + tableName + "'", e);
			}
			tableExists = true;
		}
		return table;
	}
	
	/**
	 * Remove succeeded from the head (but due to concurrency, there may still be some left), 
	 * transit those timed out to TIMED_OUT state,
	 * and remove the last transaction if it is a failed one with a null end position.
	 * @return true if data is consistent, false if data needs to be reloaded due to concurrency
	 * @param transactionEntities	 The list of transaction entities. The list may be changed inside this method.
	 * @throws TransactionStorageInfrastructureException 	if failed to update entities during the compact process
	 */
	protected boolean compact(LinkedList<SequentialTransactionWrapper> transactionEntities) throws TransactionStorageInfrastructureException{
		// remove finished historical transactions and leave only one of them
		int finished = 0; 		// 0 - no successful; 1 - one successful; 2 - two successful; ...
		Iterator<SequentialTransactionWrapper> iterator = transactionEntities.iterator();
		if (iterator.hasNext()){
			SequentialTransactionWrapper wrapper;
			wrapper = iterator.next();
			wrapper.updateFromEntity();
			if (wrapper.getTransaction().isFinished()){
				finished ++;
				while(iterator.hasNext()){
					wrapper = iterator.next();
					wrapper.updateFromEntity();
					if (wrapper.getTransaction().isFinished()){
						finished ++;
					}else{
						break;
					}
				}
			}
		}
		
		// update lastSucceededTransactionCached
		if (finished > 0){
			this.lastSucceededTransactionCached = SimpleSequentialTransaction.copyOf(
					transactionEntities.get(finished - 1).getTransaction());
		}
		
		// purge historical finished
		CloudTable table = getTableReference();
		while (finished -- > 1){
			SequentialTransactionWrapper first = transactionEntities.getFirst();
			SequentialTransactionWrapper second = first.next;
			
			// do in a transaction: remove the first one, and update the second one
			TableBatchOperation batchOperation = new TableBatchOperation();
			batchOperation.add(TableOperation.delete(first.getEntity()));
			second.setFirstTransaction();
			batchOperation.add(TableOperation.replace(second.getEntity()));
			try{
				table.execute(batchOperation);
			}catch(StorageException e){
				if (e.getHttpStatusCode() == 404){	// the first or the second had been deleted by others
					// safe to keep them in memory for now
					transactionEntities.removeFirst();
					break;
				}
				throw new TransactionStorageInfrastructureException("Failed to remove succeeded transaction entity with keys '" + first.entityKeysToString() 
						+ "' and make the next entity with keys '" + second.entityKeysToString() 
						+ "' the new first one.", e);
			}
			transactionEntities.removeFirst();
		}
		
		// handle time out
		List<SequentialTransactionWrapper> alreadyDeleted = new LinkedList<>();   // those time out and already been deleted by others
		for (SequentialTransactionWrapper wrapper: transactionEntities){
			try{
				if (!applyTimeout(wrapper)){
					alreadyDeleted.add(wrapper);
				}
			}catch(StorageException e){
				throw new TransactionStorageInfrastructureException("Failed to update timed out transaction entity with keys '" + wrapper.entityKeysToString() 
						+ "', probably it has been modified by another client.", e);
			}
		}
		
		// try to remove those already deleted, but don't leave holes
		while(transactionEntities.size() > 0 && alreadyDeleted.remove(transactionEntities.getFirst())){
			transactionEntities.removeFirst();
		}
		while(transactionEntities.size() > 0 && alreadyDeleted.remove(transactionEntities.getLast())){
			transactionEntities.removeLast();
		}
		if (alreadyDeleted.size() > 0){
			return false;		// needs a full reload
		}
		
		// if the last transaction is failed and is open, remove it
		if (transactionEntities.size() > 0){
			SequentialTransactionWrapper wrapper = transactionEntities.getLast();
			SimpleSequentialTransaction tx = wrapper.getTransactionNotNull();
			if (tx.isFailed() && tx.getEndPosition() == null){
				// do in a batch: remove the last one, make the previous one the last
				TableBatchOperation batchOperation = new TableBatchOperation();
				batchOperation.add(TableOperation.delete(wrapper.getEntity()));
				SequentialTransactionWrapper previousWrapper = wrapper.getPrevious();
				if (previousWrapper != null){
					previousWrapper.setLastTransaction();
					batchOperation.add(TableOperation.replace(previousWrapper.getEntity()));
				}
				try {
					table.execute(batchOperation);
				} catch (StorageException e) {
					if (e.getHttpStatusCode() != 404){  // ignore if someone already did the job
						throw new TransactionStorageInfrastructureException("Failed to delete failed open range transaction entity with keys '" + wrapper.entityKeysToString() 
								+ "', probably it has been modified by another client.", e);
					}
				}
				transactionEntities.removeLast();
			}
		}
		
		return true;
	}
	
	/**
	 * Check if the transaction had timed out, if yes then update the entity
	 * @param wrapper		the transaction entity wrapper
	 * @return				true if done successfully, false if the underlying entity had been deleted
	 * @throws StorageException				error when updating the entity
	 * @throws IllegalStateException		the transactions state changed and cannot be timed out
	 * @throws TransactionStorageInfrastructureException	error when getting table reference
	 */
	protected boolean applyTimeout(SequentialTransactionWrapper wrapper) throws StorageException, IllegalStateException, TransactionStorageInfrastructureException{
		CloudTable table = getTableReference();
		
		AtomicBoolean needsReload = new AtomicBoolean(false);
		return ExceptionUncheckUtility.getThrowingUnchecked(()->{
			return new AttemptStrategy(attemptStrategy)
				.overrideBackoffStrategy(BackoffStrategies.noBackoff())
				.retryIfException(StorageException.class, e-> {
					if (ENTITY_HAS_BEEN_MODIFIED_BY_OTHERS.test(e)){
						needsReload.set(true);
						return true;
					}else{
						return false;
					}
				})
				.callThrowingSuppressed(()->{
					if (needsReload.get()){
						DynamicTableEntity reloaded = fetchDynamicEntity(wrapper.getSeriesId(), wrapper.getEntityTransactionId());
						if (reloaded == null){
							return false;
						}
						wrapper.setEntity(reloaded);
						wrapper.updateFromEntity();
						needsReload.set(false);
					}
					SimpleSequentialTransaction tx = wrapper.getTransactionNotNull();
					if (tx.isInProgress() && tx.getTimeout().isBefore(Instant.now())){
						if (tx.timeout()){
							wrapper.updateToEntity();
							try{
								table.execute(TableOperation.replace(wrapper.getEntity()));
							}catch(StorageException e){
								if (e.getHttpStatusCode() == 404){
									return false;
								}else{
									throw e;
								}
							}
						}else{
							throw new IllegalStateException("Transaction '" + tx.getTransactionId() + "' in series '" + wrapper.getSeriesId() 
									+ "' is currently in " + tx.getState() + " state and cannot be changed to TIMED_OUT state");
						}
					}
					return true;
			});
		});
	}
	
	/**
	 * Fetch the transaction entity by seriesId and transactionId
	 * @param seriesId			the ID of the series that the transaction belongs to
	 * @param transactionId		the ID of the transaction
	 * @return					the transaction entity or null if not found
	 * @throws TransactionStorageInfrastructureException		if failed to get table reference
	 * @throws StorageException					if other underlying error happened
	 */
	protected SequentialTransactionEntity fetchEntity(String seriesId, String transactionId) throws TransactionStorageInfrastructureException, StorageException{
		CloudTable table = getTableReference();
		SequentialTransactionEntity entity = null;
		try{
			entity = table.execute(TableOperation.retrieve(seriesId, transactionId, SequentialTransactionEntity.class)).getResultAsType();
		}catch(StorageException e){
			if (e.getHttpStatusCode() != 404){
				throw e;
			}
		}
		return entity;
	}
	
	/**
	 * Fetch the transaction entity by transactionId only
	 * @param transactionId		the ID of the transaction
	 * @return					the transaction entity or null if not found
	 * @throws TransactionStorageInfrastructureException		if failed to get table reference
	 */
	protected SequentialTransactionEntity fetchEntity(String transactionId) throws TransactionStorageInfrastructureException{
		CloudTable table = getTableReference();
		SequentialTransactionEntity entity = null;
		entity = AzureStorageUtility.retrieveByRowKey(table, transactionId, SequentialTransactionEntity.class);
		return entity;
	}
	
	/**
	 * Fetch the transaction entity as DynamicTableEntity by seriesId and transactionId
	 * @param seriesId			the ID of the series that the transaction belongs to
	 * @param transactionId		the ID of the transaction
	 * @return					the transaction entity as DynamicTableEntity or null if not found
	 * @throws TransactionStorageInfrastructureException		if failed to get table reference
	 * @throws StorageException					if other underlying error happened
	 */
	protected DynamicTableEntity fetchDynamicEntity(String seriesId, String transactionId) throws TransactionStorageInfrastructureException, StorageException{
		CloudTable table = getTableReference();
		DynamicTableEntity entity = null;
		try{
			entity = table.execute(TableOperation.retrieve(seriesId, transactionId, DynamicTableEntity.class)).getResultAsType();
		}catch(StorageException e){
			if (e.getHttpStatusCode() != 404){
				throw e;
			}
		}
		return entity;
	}
	
	/**
	 * Fetch the transaction entity as DynamicTableEntity by transactionId only
	 * @param transactionId		the ID of the transaction
	 * @return					the transaction entity as DynamicTableEntity or null if not found
	 * @throws TransactionStorageInfrastructureException		if failed to get table reference
	 */
	protected DynamicTableEntity fetchDynamicEntity(String transactionId) throws TransactionStorageInfrastructureException{
		CloudTable table = getTableReference();
		DynamicTableEntity entity = null;
		entity = AzureStorageUtility.retrieveByRowKey(table, transactionId);
		return entity;
	}
	
	protected SequentialTransactionEntity fetchLastTransactionEntity(String seriesId) throws TransactionStorageInfrastructureException, StorageException{
		CloudTable table = getTableReference();
		SequentialTransactionEntity result = null;
		TableQuery<SequentialTransactionEntity> query = TableQuery.from(SequentialTransactionEntity.class).
				where(TableQuery.combineFilters(
						TableQuery.generateFilterCondition(
								AzureStorageUtility.PARTITION_KEY, 
								QueryComparisons.EQUAL,
								seriesId),
						TableQuery.Operators.AND,
						TableQuery.generateFilterCondition(
								"Next", 
								QueryComparisons.EQUAL,
								"")
						));
		for (SequentialTransactionEntity entity: table.execute(query)){
			if (result != null){
				throw new TransactionStorageInfrastructureException("Corrupted data for series '" + seriesId + "' in table " + tableName 
						+ ", there are at least two last transactions: " + result.keysToString() + ", " + entity.keysToString());
			}
			result = entity;
		}
		return result;
	}
	

	
	/**
	 * Fetch all entities belonging to a series into a map of SequentialTransactionEntityWrapper indexed by transaction ID
	 * @param seriesId								the ID of the series
	 * @param putAdditionalFirstTransactionEntry	When true, one additional entry will be put into the result map. The entry will have a key of null, 
	 * 												and the value will be the wrapper of the first transaction.
	 * @return		a map of SequentialTransactionEntityWrapper indexed by transaction ID, if putAdditionalFirstTransactionEntry argument is true
	 * 				there will be one more additional entry for the first transaction.
	 * @throws TransactionStorageInfrastructureException		if failed to fetch entities
	 */
	protected Map<String, SequentialTransactionWrapper> fetchEntities(String seriesId, boolean putAdditionalFirstTransactionEntry) throws TransactionStorageInfrastructureException{
		// fetch entities by seriesId
		Map<String, SequentialTransactionWrapper> map = new HashMap<>();

		try{
			CloudTable table = getTableReference();
			TableQuery<DynamicTableEntity> query = TableQuery.from(DynamicTableEntity.class).
					where(TableQuery.generateFilterCondition(
							AzureStorageUtility.PARTITION_KEY, 
							QueryComparisons.EQUAL,
							seriesId));
			for (DynamicTableEntity entity: table.execute(query)){
				SequentialTransactionWrapper wrapper = new SequentialTransactionWrapper(entity);
				/*
				if (wrapper.isFirstTransaction()){
					wrapper.setFirstTransaction();
				}
				if (wrapper.isLastTransaction()){
					wrapper.setLastTransaction();
				}*/
				map.put(wrapper.getEntity().getRowKey(), wrapper);		// indexed by transaction id
			}
		}catch(Exception e){
			throw new TransactionStorageInfrastructureException("Failed to fetch entities belonging to series '" + seriesId + "' in table " + tableName, e);
		}
		
		// sanity check
		int numFirst = 0;
		int numLast = 0;
		for (SequentialTransactionWrapper wrapper: map.values()){
			if (wrapper.isFirstTransaction()){
				numFirst ++;
			}
			if (wrapper.isLastTransaction()){
				numLast ++;
			}
		}
		if (!(numFirst == 0 && numLast == 0 || numFirst == 1 && numLast == 1)){
			String errorMsg = "Corrupted data for series '" + seriesId + "' in table " + tableName 
					+ ", number of first transaction(s): " + numFirst + ", number of last transaction(s): " + numLast;
			logger.error(errorMsg + ", transactions: {}", toList(map));
			throw new TransactionStorageInfrastructureException(errorMsg);
		}

		// link them up, with sanity check
		SequentialTransactionWrapper first = null;
		for (SequentialTransactionWrapper wrapper: map.values()){
			if (wrapper.isFirstTransaction()){
				first = wrapper;
			}else{
				String previousId = wrapper.getPreviousTransactionId();
				SequentialTransactionWrapper previous = map.get(previousId);
				if (previous == null){
					throw new TransactionStorageInfrastructureException("Corrupted data for series '" + seriesId + "' in table " + tableName
							+ ", previous transaction ID '" + previousId + "' of transaction '" + wrapper.getEntity().getRowKey() + "' cannot be found");
				}
				wrapper.setPrevious(previous);
			}
			if (!wrapper.isLastTransaction()){
				String nextId = wrapper.getNextTransactionId();
				SequentialTransactionWrapper next = map.get(nextId);
				if (next == null){
					throw new TransactionStorageInfrastructureException("Corrupted data for series '" + seriesId + "' in table " + tableName
							+ ", next transaction ID '" + nextId + "' of transaction '" + wrapper.getEntity().getRowKey() + "' cannot be found");
				}
				wrapper.setNext(next);
			}
		}
		
		if (putAdditionalFirstTransactionEntry && first != null){
			map.put(null, first);
		}
		return map;
	}
	
	/**
	 * Convert the map of SequentialTransactionEntityWrapper into a list of SequentialTransactionWrapper.
	 * @param map	the map returned by {@link #fetchEntities(String, boolean)}
	 * @return		The list, with oldest transaction as the first element and latest transaction as the last element
	 */
	protected LinkedList<SequentialTransactionWrapper> toList(Map<String, SequentialTransactionWrapper> map){
		LinkedList<SequentialTransactionWrapper> list = new LinkedList<>();
		SequentialTransactionWrapper first = map.get(null);
		if (first == null){
			Optional<SequentialTransactionWrapper> optionalFirst =
					map.values().stream().filter(wrapper -> wrapper.isFirstTransaction()).findFirst();
			first = optionalFirst.orElse(null);
		}
		if (first != null){
			SequentialTransactionWrapper wrapper = first;
			do{
				list.add(wrapper);
				wrapper = wrapper.getNext();
			}while(wrapper != null);
		}
		return list;
	}

}
