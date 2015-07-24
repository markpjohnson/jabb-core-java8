/**
 * 
 */
package net.sf.jabb.txprogress.azure;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import net.sf.jabb.txprogress.BasicProgressTransaction;
import net.sf.jabb.txprogress.ProgressTransaction;
import net.sf.jabb.txprogress.ReadOnlyProgressTransaction;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.ex.DuplicatedTransactionIdException;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.NoSuchTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableRequestOptions;

/**
 * Transactional progress tracker backed by Microsoft Azure services
 * @author James Hu
 *
 */
public class AzureTransactionalProgress implements TransactionalProgress {

	public static final String DEFAULT_TABLE_NAME = "TransactionalProgresses";
	protected String tableName = DEFAULT_TABLE_NAME;
	protected CloudTableClient tableClient;
	
	protected volatile BasicProgressTransaction lastSucceededTransactionCached;
	
	public AzureTransactionalProgress(){
		
	}
	
	public AzureTransactionalProgress(CloudStorageAccount storageAccount, String tableName, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		tableClient = storageAccount.createCloudTableClient();
		if (defaultOptionsConfigurer != null){
			defaultOptionsConfigurer.accept(tableClient.getDefaultRequestOptions());
		}
	}
	
	public AzureTransactionalProgress(CloudStorageAccount storageAccount, String tableName){
		this(storageAccount, tableName, null);
	}

	public AzureTransactionalProgress(CloudStorageAccount storageAccount, Consumer<TableRequestOptions> defaultOptionsConfigurer){
		this(storageAccount, null, defaultOptionsConfigurer);
	}

	public AzureTransactionalProgress(CloudStorageAccount storageAccount){
		this(storageAccount, null, null);
	}
	
	public AzureTransactionalProgress(CloudTableClient tableClient, String tableName){
		this();
		if (tableName != null){
			this.tableName = tableName;
		}
		this.tableClient = tableClient;
	}

	public AzureTransactionalProgress(CloudTableClient tableClient){
		this(tableClient, null);
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
	}

	@Override
	public ProgressTransaction startTransaction(String progressId,
			String previousTransactionId,
			ReadOnlyProgressTransaction transaction,
			int maxInProgressTransacions, int maxRetryingTransactions)
			throws InfrastructureErrorException,
			DuplicatedTransactionIdException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void finishTransaction(String progressId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException {
		// update a single transaction
		
	}

	@Override
	public void abortTransaction(String progressId, String processorId,
			String transactionId) throws NotOwningTransactionException,
			InfrastructureErrorException, IllegalTransactionStateException,
			NoSuchTransactionException {
		// update a single transaction
		
	}

	@Override
	public void renewTransactionTimeout(String progressId, String processorId,
			String transactionId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException,
			IllegalTransactionStateException, NoSuchTransactionException {
		// update a single transaction
		
	}

	@Override
	public boolean isTransactionSuccessful(String progressId,
			String transactionId, Instant beforeWhen)
			throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<? extends ReadOnlyProgressTransaction> getRecentTransactions(
			String progressId) throws InfrastructureErrorException {
		// get entities by progressId
		// compact the list
		return null;
	}

	@Override
	public void clear(String progressId) throws InfrastructureErrorException {
		// delete entities by progressId
		
	}

	@Override
	public void clearAll() throws InfrastructureErrorException {
		// delete all entities
		
	}



}
