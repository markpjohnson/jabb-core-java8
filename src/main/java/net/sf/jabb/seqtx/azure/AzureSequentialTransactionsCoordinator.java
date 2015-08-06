/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.seqtx.ex.SequentialTransactionException;
import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.ConsumerThrowsExceptions;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.PredicateThrowsExceptions;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.retry.AttemptStrategy;
import net.sf.jabb.util.retry.StopStrategies;

import com.google.common.base.Throwables;
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
import com.microsoft.azure.storage.table.TableServiceException;

/**
 * The implementation of SequentialTransactionsCoordinator that is backed by Microsoft Azure table storage.
 * The existence of the underlying table is checked and ensured only once during the life time of the instance of this class.
 * @author James Hu
 *
 */
public class AzureSequentialTransactionsCoordinator implements SequentialTransactionsCoordinator {
	static private final Logger logger = LoggerFactory.getLogger(AzureSequentialTransactionsCoordinator.class);

	/**
	 * The default attempt strategy for Azure operations, with maximum 60 seconds allowed in total, and 0.5 to 10 seconds backoff interval 
	 * according to fibonacci series.
	 */
	static public final AttemptStrategy DEFAULT_ATTEMPT_STRATEGY = new AttemptStrategy()
		.withWaitStrategy(WaitStrategies.threadSleepStrategy())
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(60)))
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(500L, 1000L * 10));
	
	public static final String DEFAULT_TABLE_NAME = "SequentialTransactionsCoordinator";
	public static final String DUMMY_FIRST_TRANSACTION_ID = "DUMMY_FIRST_TRANSACTION_ID" + "|||||||";
	
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile SimpleSequentialTransaction lastSucceededTransactionCached;
	protected volatile boolean tableExists = false;
	
	protected AttemptStrategy attemptStrategy = DEFAULT_ATTEMPT_STRATEGY;
	
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

	@Override
	public SequentialTransaction startTransaction(String seriesId,
			String previousTransactionId,
			ReadOnlySequentialTransaction transaction,
			int maxInProgressTransacions, int maxRetryingTransactions)
			throws InfrastructureErrorException,
			DuplicatedTransactionIdException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transaction.getProcessorId(), "Processor ID cannot be null");
		Validate.notNull(transaction.getTimeout(), "Transaction time out cannot be null");
		if (transaction.getStartPosition() == null){	// startPosition is not null when restarting a specific transaction
			Validate.isTrue(null == transaction.getEndPosition(), "End position must be null when start position is null");
		}
		Validate.isTrue(maxInProgressTransacions > 0, "Maximum number of in-progress transactions must be greater than zero: %d", maxInProgressTransacions);
		Validate.isTrue(maxRetryingTransactions > 0, "Maximum number of retrying transactions must be greater than zero: %d", maxRetryingTransactions);
		Validate.isTrue(maxInProgressTransacions >= maxRetryingTransactions, "Maximum number of in-progress transactions must not be less than the maximum number of retrying transactions: %d, %d", maxInProgressTransacions, maxRetryingTransactions);

		// try to pick up a failed one for retrying
		SequentialTransaction retryingTransaction = startAnyFailedTransaction(seriesId, transaction.getProcessorId(), transaction.getTimeout(), maxInProgressTransacions, maxRetryingTransactions);
		if (retryingTransaction != null){
			return retryingTransaction;
		}
		
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		TransactionCounts counts = SequentialTransactionsCoordinator.getTransactionCounts(transactions);
		ReadOnlySequentialTransaction last = transactions.size() > 0 ? transactions.get(transactions.size() - 1) : null;

		if (counts.getInProgress() > 0 && last.getEndPosition() == null && last.isInProgress()){  // the last one is in-progress and is open
			return null;
		}
		
		if (transaction.getStartPosition() == null){		// the client has nothing in mind, so propose a new one
			SimpleSequentialTransaction tx;
			if (last != null){
				tx = new SimpleSequentialTransaction(last.getTransactionId(), transaction.getProcessorId(), last.getEndPosition(), transaction.getTimeout());
			}else{
				tx = new SimpleSequentialTransaction(null, transaction.getProcessorId(), null, transaction.getTimeout());
			}
			return tx;
		}else{		// try to start the transaction requested by the client
			if (last == null || last.getTransactionId().equals(previousTransactionId)){
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
				try {
					SequentialTransactionEntity createdEntity = createNewTransaction(seriesId, previousTransactionId, newTrans);
					return createdEntity.toSequentialTransaction();
				} catch (IllegalStateException e) {	// the last one is no longer the last
					SequentialTransactionEntity latestLast = null;
					try{
						latestLast = fetchLastTransactionEntity(seriesId);
					}catch(Exception e1){
						throw new InfrastructureErrorException("Failed to fetch latest last transaction in series '" + seriesId + "'", e1);
					}
					return new SimpleSequentialTransaction(latestLast.getTransactionId(), transaction.getProcessorId(), latestLast.getEndPosition(), transaction.getTimeout());
				} catch (StorageException e){
					throw new InfrastructureErrorException("Failed to create after the last one with ID '" + previousTransactionId + "' a new transaction: " + newTrans, e);
				}
			}else{
				// propose a new one
				return new SimpleSequentialTransaction(last.getTransactionId(), transaction.getProcessorId(), last.getEndPosition(), transaction.getTimeout());
			}
		}
	}
	
	/**
	 * Create a new transaction after the last one
	 * @param seriesId					ID of the series
	 * @param lastTransactionId			ID of the last transaction
	 * @param newTrans					the new transaction
	 * @return the transaction entity created
	 * @throws IllegalStateException	if the last transaction is no longer the last
	 * @throws StorageException			any error happened when updating the last and inserting the new one
	 * @throws InfrastructureErrorException		if unable to get table reference
	 */
	protected SequentialTransactionEntity createNewTransaction(String seriesId, String lastTransactionId, SimpleSequentialTransaction newTrans) throws IllegalStateException, StorageException, InfrastructureErrorException{
		CloudTable table = getTableReference();
		SequentialTransactionEntity last = null;
		if (lastTransactionId == null){ // the first one
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
		}else{
			last = fetchEntity(seriesId, lastTransactionId);
			if (!last.isLastTransaction()){
				throw new IllegalStateException("The transaction is no longer the last one: " + last.keysToString());
			}
		}
		SequentialTransactionEntity next = SequentialTransactionEntity.fromSequentialTransaction(seriesId, newTrans, last.getTransactionId(), null);
		last.setNextTransactionId(next.getTransactionId());
		next.setLastTransaction();
		
		// do in a transaction: update the last, and insert the new one
		TableBatchOperation batchOperation = new TableBatchOperation();
		batchOperation.add(TableOperation.merge(last));
		batchOperation.add(TableOperation.insert(next));
		table.execute(batchOperation);
		
		return next;
		// TODO: check the duplicated keys exception
	}
	
	@Override
	public SequentialTransaction startAnyFailedTransaction(String seriesId, String processorId, Instant timeout, int maxInProgressTransacions, int maxRetryingTransactions)
			throws InfrastructureErrorException{
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(timeout, "Transaction time out cannot be null");
		Validate.isTrue(maxInProgressTransacions > 0, "Maximum number of in-progress transactions must be greater than zero: %d", maxInProgressTransacions);
		Validate.isTrue(maxRetryingTransactions > 0, "Maximum number of retrying transactions must be greater than zero: %d", maxRetryingTransactions);
		Validate.isTrue(maxInProgressTransacions >= maxRetryingTransactions, "Maximum number of in-progress transactions must not be less than the maximum number of retrying transactions: %d, %d", maxInProgressTransacions, maxRetryingTransactions);

		try{
			return new AttemptStrategy(attemptStrategy)
				.retryIfException(IllegalTransactionStateException.class)
				.retryIfException(NoSuchTransactionException.class)
				.callThrowingSuppressed(()->{return doStartAnyFailedTransaction(seriesId, processorId, timeout, maxInProgressTransacions, maxRetryingTransactions);});
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to start failed transaction for retrying: " + seriesId, e);
		}
	}
	
	protected SequentialTransaction doStartAnyFailedTransaction(String seriesId, String processorId, Instant timeout, int maxInProgressTransacions, int maxRetryingTransactions)
			throws InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, Exception{
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactionsIncludingDummy(seriesId);
		TransactionCounts counts = SequentialTransactionsCoordinator.getTransactionCounts(transactions);

		if (counts.getInProgress() >= maxInProgressTransacions){  // no more transaction allowed
			return null;
		}
		
		if (counts.getRetrying() < maxRetryingTransactions && counts.getFailed() > 0){	// always first try to pick up a failed to retry
			Optional<? extends ReadOnlySequentialTransaction> firstFailed = transactions.stream().filter(tx->tx.isFailed()).findFirst();
			if (firstFailed.isPresent()){
				ReadOnlySequentialTransaction tx = firstFailed.get();
				String transactionKey = AzureStorageUtility.keysToString(seriesId, tx.getTransactionId());
				AtomicReference<SequentialTransaction> startedTx = new AtomicReference<>(null);
				try {
					new AttemptStrategy(attemptStrategy)
						.retryIfException(StorageException.class, e-> e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode()))
						.runThrowingSuppressed(()->modifyTransaction(seriesId, null, tx.getTransactionId(), 
								entity->entity.retry(processorId, timeout), entity->{
									CloudTable table = getTableReference();
									table.execute(TableOperation.replace(entity));
									startedTx.set(entity.toSequentialTransaction());
								}));
				} catch (InfrastructureErrorException e){
					throw e;
				} catch (IllegalTransactionStateException | NoSuchTransactionException e){ // picked up by someone else
					throw e;
				} catch (Exception e){	// only possible: StorageException|RuntimeException
					throw new InfrastructureErrorException("Failed to update transaction entity for retry: " + transactionKey, e);
				}

				return startedTx.get();
			}
		}
		return null;	// no transaction can be started for retrying
	}

	@Override
	public void finishTransaction(String seriesId, String processorId,
			String transactionId, String endPosition) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException, IllegalEndPositionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction ID cannot be null");

		String transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
		try {
			AtomicReference<String> updatedEndPosition = new AtomicReference<>(null);
			new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, e-> e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode()))
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->{
							updatedEndPosition.set(entity.getEndPosition());
							if (endPosition != null){
								if (entity.isLastTransaction()){
									updatedEndPosition.set(endPosition);
								}else{
									if (!endPosition.equals(entity.getEndPosition())){
										// can't change the end position of a non-last transaction
										throw new IllegalEndPositionException("Cannot change transaction end position from '" + entity.getEndPosition() + "' to '" + endPosition + "' because it is not the last transaction: " + transactionKey);
									}
								}
							}
							if (updatedEndPosition.get() == null){
								// cannot finish an open transaction
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
									throw new IllegalTransactionStateException("Open range transaction may already have timed out and then been deleted: " + entity.keysToString());
								}else{
									throw e;
								}
							}
						}));
		} catch (NotOwningTransactionException | InfrastructureErrorException | IllegalTransactionStateException | IllegalEndPositionException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new InfrastructureErrorException("Failed to update transaction entity state to " + SequentialTransactionState.FINISHED + ": " + transactionKey, e);
		}
	}

	@Override
	public void abortTransaction(String seriesId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");

		String transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
		try {
			new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, e-> e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode()))
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->entity.abort(), entity->{
							CloudTable table = getTableReference();
							table.execute(TableOperation.replace(entity));
						}));
		} catch (NotOwningTransactionException | InfrastructureErrorException | IllegalTransactionStateException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new InfrastructureErrorException("Failed to update transaction entity state to " + SequentialTransactionState.ABORTED + ": " + transactionKey, e);
		}
	}

	@Override
	public void renewTransactionTimeout(String seriesId, String processorId,
			String transactionId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException,
			IllegalTransactionStateException, NoSuchTransactionException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(processorId, "Processor ID cannot be null");
		Validate.notNull(timeout, "Transaction time out cannot be null");

		String transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
		try {
			new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, e-> e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode()))
				.runThrowingSuppressed(()->modifyTransaction(seriesId, processorId, transactionId, 
						entity->entity.isInProgress(), entity->{
							entity.setTimeout(timeout);
							CloudTable table = getTableReference();
							table.execute(TableOperation.replace(entity));
						}));
		} catch (NotOwningTransactionException | InfrastructureErrorException | IllegalTransactionStateException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException|RuntimeException
			throw new InfrastructureErrorException("Failed to update timeout property for transaction entity with keys: " + transactionKey, e);
		}
			
	}
	
	/**
	 * Perform modification of a transaction
	 * @param seriesId							ID of the series
	 * @param processorId						ID of the process that this transaction must belong to, or null if there is no need to check this
	 * @param transactionId						ID of the transaction
	 * @param stateChecker						lambda to check whether the transaction state is okay, returns true for ok false for throwing IllegalTransactionStateException
	 * @param updater							lambda to perform the update, StorageException is the only unchecked exception allowed to be thrown
	 * @throws NotOwningTransactionException		if the transaction is not currently owned by the process with specified processId
	 * @throws InfrastructureErrorException			if failed to update the entity
	 * @throws IllegalTransactionStateException		if the state of the transaction is not IN_PROGRESS
	 * @throws NoSuchTransactionException	if no such transaction can be found
	 * @throws StorageException				if failed to replace the entity with updated values
	 */
	protected void modifyTransaction(String seriesId, String processorId, String transactionId, 
			PredicateThrowsExceptions<SequentialTransactionEntity> stateChecker, ConsumerThrowsExceptions<SequentialTransactionEntity> updater)
			throws NotOwningTransactionException, InfrastructureErrorException,
			IllegalTransactionStateException, NoSuchTransactionException, StorageException {
		// update a single transaction
		String transactionKey = AzureStorageUtility.keysToString(seriesId, transactionId);
		SequentialTransactionEntity entity = null;
		try {
			entity = fetchEntity(seriesId, transactionId);
		} catch (StorageException e) {
			throw new InfrastructureErrorException("Failed to fetch transaction entity with keys: " + transactionKey, e);
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
	public boolean isTransactionSuccessful(String seriesId,
			String transactionId, Instant beforeWhen)
			throws InfrastructureErrorException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		Validate.notNull(transactionId, "Transaction time out cannot be null");
		Validate.notNull(beforeWhen, "Time cannot be null");

		// try cached first
		if (lastSucceededTransactionCached != null){
			if (!beforeWhen.isAfter(lastSucceededTransactionCached.getFinishTime())){
				return true;
			}
		}
		
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
		
		return true;		// must be a very old transaction
	}

	protected List<? extends ReadOnlySequentialTransaction> getRecentTransactionsIncludingDummy(
			String seriesId) throws InfrastructureErrorException {
		// TODO: attemptStrategy
		// get entities by seriesId
		Map<String, SequentialTransactionWrapper> wrappedTransactionEntities = fetchEntities(seriesId, true);
		LinkedList<SequentialTransactionWrapper> transactionEntities = toList(wrappedTransactionEntities);
		
		// compact the list
		compact(transactionEntities);
		
		return transactionEntities.stream().map(SequentialTransactionWrapper::getTransactionNotNull).collect(Collectors.toList());
	}

	@Override
	public List<? extends ReadOnlySequentialTransaction> getRecentTransactions(
			String seriesId) throws InfrastructureErrorException {
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
	public void clear(String seriesId) throws InfrastructureErrorException {
		Validate.notNull(seriesId, "Series ID cannot be null");
		// delete entities by seriesId
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExist(table, 
					TableQuery.generateFilterCondition(
							AzureStorageUtility.PARTITION_KEY, 
							QueryComparisons.EQUAL,
							seriesId));
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to delete entities belonging to series '" + seriesId + "' in table " + tableName, e);
		}
	}

	@Override
	public void clearAll() throws InfrastructureErrorException {
		// delete all entities
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExist(table, (String)null);
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to delete all entities in table " + tableName, e);
		}
	}
	
	protected CloudTable getTableReference() throws InfrastructureErrorException{
		CloudTable table;
		try {
			table = tableClient.getTableReference(tableName);
		} catch (Exception e) {
			throw new InfrastructureErrorException("Failed to get reference for table '" + tableName + "'", e);
		}
		if (!tableExists){
			try {
				AzureStorageUtility.createIfNotExist(tableClient, tableName);
			} catch (Exception e) {
				throw new InfrastructureErrorException("Failed to ensure the existence of table '" + tableName + "'", e);
			}
			tableExists = true;
		}
		return table;
	}
	
	/**
	 * Remove succeeded from the head and leave only one, transit those timed out to TIMED_OUT state,
	 * and remove the last transaction if it is a failed one with a null end position.
	 * @param transactionEntities	 The list of transaction entities. The list may be changed inside this method.
	 * @throws InfrastructureErrorException 	if failed to update entities during the compact process
	 */
	protected void compact(LinkedList<SequentialTransactionWrapper> transactionEntities) throws InfrastructureErrorException{
		// remove finished historical transactions and leave only one of them
		int finished = 0;
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
				AzureStorageUtility.executeIfExist(table, batchOperation);
			}catch(StorageException e){
				// TODO: attemptStrategy
				throw new InfrastructureErrorException("Failed to remove succeeded transaction entity with keys '" + first.entityKeysToString() 
						+ "' and make the next entity with keys '" + second.entityKeysToString() 
						+ "' the new first one, probably one of them has been modified by another client.", e);
			}
			transactionEntities.removeFirst();
		}
		
		// update lastSucceededTransactionCached
		if (transactionEntities.size() > 0){
			SequentialTransactionWrapper first = transactionEntities.getFirst();
			if (first.getTransactionNotNull().isFinished()){
				this.lastSucceededTransactionCached = SimpleSequentialTransaction.copyOf(first.getTransaction());
			}
		}
		
		// handle time out
		List<SequentialTransactionWrapper> toBeDeleted = new LinkedList<>();
		for (SequentialTransactionWrapper wrapper: transactionEntities){
			try{
				if (!applyTimeout(wrapper)){
					toBeDeleted.add(wrapper);
				}
			}catch(StorageException e){
				throw new InfrastructureErrorException("Failed to update timed out transaction entity with keys '" + wrapper.entityKeysToString() 
						+ "', probably it has been modified by another client.", e);
			}
		}
		transactionEntities.removeAll(toBeDeleted);
		
		// if the last transaction is failed and is open, remove it
		if (transactionEntities.size() > 0){
			SequentialTransactionWrapper wrapper = transactionEntities.getLast();
			SimpleSequentialTransaction tx = wrapper.getTransactionNotNull();
			if (tx.isFailed() && tx.getEndPosition() == null){
				try {
					AzureStorageUtility.deleteEntitiesIfExist(table, wrapper.getEntity());
				} catch (StorageException e) {
					throw new InfrastructureErrorException("Failed to delete failed open range transaction entity with keys '" + wrapper.entityKeysToString() 
							+ "', probably it has been modified by another client.", e);
				}
				transactionEntities.removeLast();
			}
		}
	}
	
	protected boolean applyTimeout(SequentialTransactionWrapper wrapper) throws StorageException, IllegalStateException, InfrastructureErrorException{
		CloudTable table = getTableReference();
		Instant now = Instant.now();
		
		AtomicBoolean needsReload = new AtomicBoolean(false);
		return ExceptionUncheckUtility.getThrowingUnchecked(()->{
			return new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, e-> {
					if (e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.UPDATE_CONDITION_NOT_SATISFIED.equals(e.getErrorCode())){
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
					if (tx.isInProgress() && tx.getTimeout().isBefore(now)){
						if (tx.timeout()){
							wrapper.updateToEntity();
							table.execute(TableOperation.replace(wrapper.getEntity()));
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
	 * @throws InfrastructureErrorException		if failed to get table reference
	 * @throws StorageException					if other underlying error happened
	 */
	protected SequentialTransactionEntity fetchEntity(String seriesId, String transactionId) throws InfrastructureErrorException, StorageException{
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
	 * Fetch the transaction entity as DynamicTableEntity by seriesId and transactionId
	 * @param seriesId			the ID of the series that the transaction belongs to
	 * @param transactionId		the ID of the transaction
	 * @return					the transaction entity as DynamicTableEntity or null if not found
	 * @throws InfrastructureErrorException		if failed to get table reference
	 * @throws StorageException					if other underlying error happened
	 */
	protected DynamicTableEntity fetchDynamicEntity(String seriesId, String transactionId) throws InfrastructureErrorException, StorageException{
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
	
	protected SequentialTransactionEntity fetchLastTransactionEntity(String seriesId) throws InfrastructureErrorException, StorageException{
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
				throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName 
						+ ", there are at least two last transactions: " + result.keysToString() + ", " + entity.keysToString());
			}
			return result = entity;
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
	 * @throws InfrastructureErrorException		if failed to fetch entities
	 */
	protected Map<String, SequentialTransactionWrapper> fetchEntities(String seriesId, boolean putAdditionalFirstTransactionEntry) throws InfrastructureErrorException{
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
			throw new InfrastructureErrorException("Failed to fetch entities belonging to series '" + seriesId + "' in table " + tableName, e);
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
			throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName 
					+ ", number of first transaction(s): " + numFirst + ", number of last transaction(s): " + numLast);
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
					throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName
							+ ", previous transaction ID '" + previousId + "' of transaction '" + wrapper.getEntity().getRowKey() + "' cannot be found");
				}
				wrapper.setPrevious(previous);
			}
			if (!wrapper.isLastTransaction()){
				String nextId = wrapper.getNextTransactionId();
				SequentialTransactionWrapper next = map.get(nextId);
				if (next == null){
					throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName
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
