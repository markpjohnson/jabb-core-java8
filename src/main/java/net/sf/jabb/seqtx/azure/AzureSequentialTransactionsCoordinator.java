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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jabb.azure.AzureStorageUtility;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.retry.AttemptStrategy;
import net.sf.jabb.util.retry.StopStrategies;

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
	 * The default attempt strategy for Azure operations, with maximum 2 minutes allowed in total, and 1 to 10 seconds backoff interval 
	 * according to fibonacci series.
	 */
	static public final AttemptStrategy DEFAULT_ATTEMPT_STRATEGY = new AttemptStrategy()
		.withWaitStrategy(WaitStrategies.threadSleepStrategy())
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofMinutes(1)))
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(500L, 1000L * 10));
	
	public static final String DEFAULT_TABLE_NAME = "SequentialTransactionsCoordinator";
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void finishTransaction(String seriesId, String processorId,
			String transactionId, String endPosition) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException {
		// update a single transaction
		
	}

	@Override
	public void abortTransaction(String seriesId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException {
		// update a single transaction
		
	}

	@Override
	public void renewTransactionTimeout(String seriesId, String processorId,
			String transactionId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException,
			IllegalTransactionStateException, NoSuchTransactionException {
		try {
			new AttemptStrategy(attemptStrategy)
				.retryIfException(StorageException.class, e-> e.getHttpStatusCode() == 412 && StorageErrorCodeStrings.CONDITION_NOT_MET.equals(e.getErrorCode()))
				.runThrowingSuppressed(()->doRenewTransactionTimeout(seriesId, processorId, transactionId, timeout));
		} catch (NotOwningTransactionException | InfrastructureErrorException | IllegalTransactionStateException | NoSuchTransactionException e){
			throw e;
		} catch (Exception e){	// only possible: StorageException
			throw new InfrastructureErrorException("Failed to update timeout property for transaction entity with keys: " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
			
	}
	
	/**
	 * Perform timeout renew of a transaction
	 * @param seriesId							ID of the series
	 * @param processorId						ID of the process requesting the renew
	 * @param transactionId						ID of the transaction
	 * @param timeout							the new time out
	 * @throws NotOwningTransactionException		if the transaction is not currently owned by the process with specified processId
	 * @throws InfrastructureErrorException			if failed to update the entity
	 * @throws IllegalTransactionStateException		if the state of the transaction is not IN_PROGRESS
	 * @throws NoSuchTransactionException	if no such transaction can be found
	 * @throws StorageException				if failed to replace the entity with updated values
	 */
	protected void doRenewTransactionTimeout(String seriesId, String processorId,
			String transactionId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException,
			IllegalTransactionStateException, NoSuchTransactionException, StorageException {
		// update a single transaction
		SequentialTransactionEntity entity = null;
		try {
			entity = fetchEntity(seriesId, transactionId);
		} catch (StorageException e) {
			throw new InfrastructureErrorException("Failed to fetch transaction entity with keys: " + AzureStorageUtility.keysToString(seriesId, transactionId), e);
		}
		if (entity == null){
			throw new NoSuchTransactionException("Transaction '" + transactionId + "' either does not exist or have succeeded and later been purged");
		}
		if (!processorId.equals(entity.getProcessorId())){
			throw new NotOwningTransactionException("Transaction '" + transactionId + "' is currently owned by processor '" + entity.getProcessorId() + "', not '" + processorId + "'");
		}
		
		if (entity.isInProgress()){
			entity.setTimeout(timeout);
			CloudTable table = getTableReference();
			table.execute(TableOperation.replace(entity));
		}else{
			throw new IllegalTransactionStateException("Transaction '" + transactionId + "' is currently in " + entity.getState() + " state and its timeout cannot be changed");
		}
	}

	@Override
	public boolean isTransactionSuccessful(String seriesId,
			String transactionId, Instant beforeWhen)
			throws InfrastructureErrorException {
		// try cached first
		if (lastSucceededTransactionCached != null){
			if (!beforeWhen.isAfter(lastSucceededTransactionCached.getFinishTime())){
				return true;
			}
		}
		
		// reload
		List<? extends ReadOnlySequentialTransaction> transactions = getRecentTransactions(seriesId);
		
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

	@Override
	public List<? extends ReadOnlySequentialTransaction> getRecentTransactions(
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
	public void clear(String seriesId) throws InfrastructureErrorException {
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
		Instant now = Instant.now();
		for (SequentialTransactionWrapper wrapper: transactionEntities){
			SimpleSequentialTransaction tx = wrapper.getTransactionNotNull();
			if (tx.isInProgress() && tx.getTimeout().isBefore(now)){
				if (tx.timeout()){
					wrapper.updateToEntity();
					try{
						table.execute(TableOperation.replace(wrapper.getEntity()));
					}catch(StorageException e){
						// TODO: attemptStrategy
						throw new InfrastructureErrorException("Failed to update timed out transaction entity with keys '" + wrapper.entityKeysToString() 
								+ "', probably it has been modified by another client.", e);
					}
				}else{
					throw new IllegalStateException("Transaction '" + tx.getTransactionId() + "' in series '" + wrapper.getSeriesId() 
							+ "' is currently in " + tx.getState() + " state and cannot be changed to TIMED_OUT state");
				}
			}
		}
		
		// if the last transaction is failed and is open, remove it
		if (transactionEntities.size() > 0){
			SequentialTransactionWrapper wrapper = transactionEntities.getLast();
			SimpleSequentialTransaction tx = wrapper.getTransactionNotNull();
			if (tx.isFailed() && tx.getEndPosition() == null){
				try {
					AzureStorageUtility.deleteEntitiesIfExist(table, wrapper.getEntity());
				} catch (StorageException e) {
					// TODO: attemptStrategy
					throw new InfrastructureErrorException("Failed to delete failed open range transaction entity with keys '" + wrapper.entityKeysToString() 
							+ "', probably it has been modified by another client.", e);
				}
				transactionEntities.removeLast();
			}
		}
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
				String nextId = wrapper.getPreviousTransactionId();
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
