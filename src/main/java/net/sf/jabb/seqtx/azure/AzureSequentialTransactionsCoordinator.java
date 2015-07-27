/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableRequestOptions;

/**
 * The implementation of SequentialTransactionsCoordinator that is backed by Microsoft Azure table storage
 * @author James Hu
 *
 */
public class AzureSequentialTransactionsCoordinator implements SequentialTransactionsCoordinator {
	static private final Logger logger = LoggerFactory.getLogger(AzureSequentialTransactionsCoordinator.class);

	public static final String DEFAULT_TABLE_NAME = "SequentialTransactionsCoordinator";
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile SimpleSequentialTransaction lastSucceededTransactionCached;
	protected volatile boolean tableExists = false;
	
	
	public AzureSequentialTransactionsCoordinator(){
		
	}
	
	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, String tableName, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		tableClient = storageAccount.createCloudTableClient();
		if (defaultOptionsConfigurer != null){
			defaultOptionsConfigurer.accept(tableClient.getDefaultRequestOptions());
		}
	}
	
	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, String tableName){
		this(storageAccount, tableName, null);
	}

	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this(storageAccount, null, defaultOptionsConfigurer);
	}

	public AzureSequentialTransactionsCoordinator(CloudStorageAccount storageAccount){
		this(storageAccount, null, null);
	}
	
	public AzureSequentialTransactionsCoordinator(CloudTableClient tableClient, String tableName){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		this.tableClient = tableClient;
	}

	public AzureSequentialTransactionsCoordinator(CloudTableClient tableClient){
		this(tableClient, null);
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
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
		// update a single transaction
		
	}

	@Override
	public boolean isTransactionSuccessful(String seriesId,
			String transactionId, Instant beforeWhen)
			throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<? extends ReadOnlySequentialTransaction> getRecentTransactions(
			String seriesId) throws InfrastructureErrorException {
		// get entities by seriesId
		List<SequentialTransactionEntity> entities = new LinkedList<>();
		try{
			CloudTable table = getTableReference();
			TableQuery<SequentialTransactionEntity> query = TableQuery.from(SequentialTransactionEntity.class).
					where(TableQuery.generateFilterCondition(
							AzureStorageUtility.PARTITION_KEY, 
							QueryComparisons.EQUAL,
							seriesId));
			for (SequentialTransactionEntity entity: table.execute(query)){
				entities.add(entity);
			}
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to fetch entities belonging to series '" + seriesId + "' in table " + tableName, e);
		}
		
		Collections.sort(entities, new Comparator<SequentialTransactionEntity>);
		
		// compact the list
		return null;
	}

	@Override
	public void clear(String seriesId) throws InfrastructureErrorException {
		// delete entities by seriesId
		try{
			CloudTable table = getTableReference();
			AzureStorageUtility.deleteEntitiesIfExist(table, SequentialTransactionEntity.class, 
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
			AzureStorageUtility.deleteEntitiesIfExist(table, SequentialTransactionEntity.class, null);
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to delete all entities in table " + tableName, e);
		}
	}
	
	protected CloudTable getTableReference() throws URISyntaxException, StorageException{
		CloudTable table = tableClient.getTableReference(tableName);
		if (!tableExists){
			AzureStorageUtility.createIfNotExist(tableClient, tableName);
			tableExists = true;
		}
		return table;
	}
	
	protected Map<String, SequentialTransactionEntityWrapper> fetchEntities(String seriesId) throws InfrastructureErrorException{
		// fetch entities by seriesId
		Map<String, SequentialTransactionEntityWrapper> map = new HashMap<>();

		try{
			CloudTable table = getTableReference();
			TableQuery<SequentialTransactionEntity> query = TableQuery.from(SequentialTransactionEntity.class).
					where(TableQuery.generateFilterCondition(
							AzureStorageUtility.PARTITION_KEY, 
							QueryComparisons.EQUAL,
							seriesId));
			for (SequentialTransactionEntity entity: table.execute(query)){
				if (entity.isFirstTransaction()){
					entity.setFirstTransaction();
				}
				if (entity.isLastTransaction()){
					entity.setLastTransaction();
				}
				map.put(entity.getTransactionId(), new SequentialTransactionEntityWrapper(entity));
			}
		}catch(Exception e){
			throw new InfrastructureErrorException("Failed to fetch entities belonging to series '" + seriesId + "' in table " + tableName, e);
		}
		
		// sanity check
		final AtomicInteger numFirst = new AtomicInteger(0);
		final AtomicInteger numLast = new AtomicInteger(0);
		map.values().stream().map(wrapper -> wrapper.getEntity()).forEach(entity->{
			if (entity.isFirstTransaction()){
				numFirst.incrementAndGet();
			}
			if (entity.isLastTransaction()){
				numLast.incrementAndGet();
			}
		});
		if (!(numFirst.get() == 0 && numLast.get() == 0 || numFirst.get() == 1 && numLast.get() == 1)){
			throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName 
					+ ", number of first transaction(s): " + numFirst.get() + ", number of last transaction(s): " + numLast.get());
		}

		// link them up, with sanity check
		for (SequentialTransactionEntityWrapper wrapper: map.values()){
			SequentialTransactionEntity entity = wrapper.getEntity();
			if (!entity.isFirstTransaction()){
				String previousId = entity.getPreviousTransactionId();
				SequentialTransactionEntityWrapper previous = map.get(previousId);
				if (previous == null){
					throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName
							+ ", previous transaction ID '" + previousId + "' of transaction '" + entity.getTransactionId() + "' cannot be found");
				}
				wrapper.setPrevious(previous);
			}
			if (!entity.isLastTransaction()){
				String nextId = entity.getPreviousTransactionId();
				SequentialTransactionEntityWrapper next = map.get(nextId);
				if (next == null){
					throw new InfrastructureErrorException("Corrupted data for series '" + seriesId + "' in table " + tableName
							+ ", next transaction ID '" + nextId + "' of transaction '" + entity.getTransactionId() + "' cannot be found");
				}
				wrapper.setNext(next);
			}
		}
		
		return map;
	}
	
	protected List<SequentialTransactionEntity> toList(Map<String, SequentialTransactionEntityWrapper> map){
		List<SequentialTransactionEntity> list = new LinkedList<>();
		Optional<SequentialTransactionEntityWrapper> first = map.values().stream().filter(wrapper -> wrapper.getEntity().isFirstTransaction()).findFirst();
		if (first.isPresent()){
			SequentialTransactionEntityWrapper wrapper = first.get();
			do{
				list.add(wrapper.getEntity());
				wrapper = wrapper.getNext();
			}while(wrapper != null);
		}
		return list;
	}


}
