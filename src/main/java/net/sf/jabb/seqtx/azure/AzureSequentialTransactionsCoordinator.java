/**
 * 
 */
package net.sf.jabb.seqtx.azure;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

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
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableRequestOptions;

/**
 * Transactional progress tracker backed by Microsoft Azure services
 * @author James Hu
 *
 */
public class AzureSequentialTransactionsCoordinator implements SequentialTransactionsCoordinator {

	public static final String DEFAULT_TABLE_NAME = "SequentialTransactionsCoordinator";
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile SimpleSequentialTransaction lastSucceededTransactionCached;
	
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
		// compact the list
		return null;
	}

	@Override
	public void clear(String seriesId) throws InfrastructureErrorException {
		// delete entities by seriesId
		
	}

	@Override
	public void clearAll() throws InfrastructureErrorException {
		// delete all entities
		
	}



}
