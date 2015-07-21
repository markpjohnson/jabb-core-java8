/**
 * 
 */
package net.sf.jabb.txprogress.azure;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

import net.sf.jabb.txprogress.ProgressTransaction;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.txprogress.ex.NotCurrentTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningLeaseException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;
import net.sf.jabb.txprogress.ex.TransactionTimeoutAfterLeaseExpirationException;

import org.apache.commons.lang3.Validate;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

/**
 * Transactional progress tracker backed by Microsoft Azure services
 * @author James Hu
 *
 */
public class AzureTransactionalProgress implements TransactionalProgress {
	public static final int LEASE_SECONDS_MIN = 15;
	public static final int LEASE_SECONDS_MAX = 60;
	public static final int BLOB_NAME_LENGTH_MAX = 1024;

	public static final String DEFAULT_CONTAINER_NAME = "txprogress";
	protected String containerName;
	protected CloudBlobClient blobClient;
	
	public AzureTransactionalProgress(){
		
	}
	
	public AzureTransactionalProgress(CloudStorageAccount storageAccount, String containerName, Consumer<BlobRequestOptions> defaultOptionsConfigurer){
		this();
		this.containerName = containerName == null ? DEFAULT_CONTAINER_NAME : containerName;
		blobClient = storageAccount.createCloudBlobClient();
		if (defaultOptionsConfigurer != null){
			defaultOptionsConfigurer.accept(blobClient.getDefaultRequestOptions());
		}
	}
	
	public AzureTransactionalProgress(CloudStorageAccount storageAccount, String containerPrefix){
		this(storageAccount, containerPrefix, null);
	}

	public AzureTransactionalProgress(CloudStorageAccount storageAccount){
		this(storageAccount, null, null);
	}

	/**
	 * Get the lease time in seconds and ensure it is valid for Azure Blob leasing
	 * @param leaseExpirationTimed	the time that the lease will expire
	 * @return	number of seconds of the lease period
	 * @throws IllegalArgumentException  if it is not within [15, 60] range which is required by Azure
	 */
	protected int getLeaseTimeInSeconds(Instant leaseExpirationTimed){
		Duration duration = Duration.between(Instant.now(), leaseExpirationTimed);
		int leaseTimeInSeconds =  (int)duration.getSeconds() + duration.getNano() >= 500_000_000 ? 1 : 0;
		if (leaseTimeInSeconds < LEASE_SECONDS_MIN || leaseTimeInSeconds > LEASE_SECONDS_MAX){
			throw new IllegalArgumentException("Length of lease period must be between 15 and 60 seconds inclusive.");
		}
		return leaseTimeInSeconds;
	}
	
	protected String processorIdToLeaseId(String processorId){
		Validate.notBlank(processorId, "ID of the processor cannot be blank");
		return UUID.nameUUIDFromBytes(processorId.getBytes(StandardCharsets.UTF_8)).toString();
	}
	
	protected String progressIdToBlobName(String progressId){
		Validate.notBlank(progressId, "ID of the progress cannot be blank");
		try {
			String urlEncoded = URLEncoder.encode(progressId, "UTF-8");
			Validate.isTrue(!urlEncoded.endsWith("."), "ID of the progress cannot end with '.'");
			Validate.isTrue(!urlEncoded.endsWith("/"), "ID of the progress cannot end with '/'");
			Validate.isTrue(urlEncoded.length() <= BLOB_NAME_LENGTH_MAX, "Length of the progress after URL encoding cannot exceed 1024");
			return urlEncoded;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 encoding is not supported", e);
		}
	}
	
	protected CloudBlob getBlobForProgress(String progressId) throws InfrastructureErrorException{
		String blobNameForProgress = progressIdToBlobName(progressId);
		
		CloudBlobContainer container;
		try {
			container = blobClient.getContainerReference(containerName);
		} catch (Exception e) {
			throw new InfrastructureErrorException("Failed to get container reference for '" + containerName + "' : " + e.getMessage(), e);
		}
		
		CloudBlob blob;
		try {
			blob = container.getBlockBlobReference(blobNameForProgress);
		} catch (Exception e) {
			throw new InfrastructureErrorException("Failed to get blob reference for '" + progressId + "' : " + e.getMessage(), e);
		}
		
		return blob;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#acquireLease(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean acquireLease(String progressId, String processorId, Instant leaseExpirationTimed) throws InfrastructureErrorException {
		int leaseTimeInSeconds = getLeaseTimeInSeconds(leaseExpirationTimed);
		String leaseIdForProcessor = processorIdToLeaseId(processorId);
		
		CloudBlob blob = getBlobForProgress(progressId);
		
		String leaseId;
		try {
			leaseId = blob.acquireLease(leaseTimeInSeconds, leaseIdForProcessor);
		} catch (StorageException e) {
			if (e.getHttpStatusCode() == 409){	// leased by others.   https://msdn.microsoft.com/en-us/library/azure/dd179439.aspx
				return false;
			}
			throw new InfrastructureErrorException("Failed to acquire lease for progress '" + progressId + "' and processor '" + processorId + "' : " + e.getMessage(), e);
		}
		
		if (leaseId.equals(leaseIdForProcessor)){
			return true;
		}else{
			throw new InfrastructureErrorException("When acquiring, proposed lease ID '" + leaseIdForProcessor + "' is not adopted by Azure, returned lease ID is '" + leaseId + "'");
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#renewLease(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean renewLease(String progressId, String processorId, Instant leaseExpirationTimed) throws InfrastructureErrorException {
		int leaseTimeInSeconds = getLeaseTimeInSeconds(leaseExpirationTimed);
		String leaseIdForProcessor = processorIdToLeaseId(processorId);
		
		CloudBlob blob = getBlobForProgress(progressId);
		
		String leaseId;
		try {
			AccessCondition condition = new AccessCondition();
			condition.setLeaseID(leaseIdForProcessor);
			leaseId = blob.acquireLease(leaseTimeInSeconds, leaseIdForProcessor, condition, null, null);
		} catch (StorageException e) {
			if (e.getHttpStatusCode() == 409 || e.getHttpStatusCode() == 412){	// the lease is not valid.   https://msdn.microsoft.com/en-us/library/azure/dd179439.aspx
				return false;
			}
			throw new InfrastructureErrorException("Failed to renew lease for progress '" + progressId + "' and processor '" + processorId + "' : " + e.getMessage(), e);
		}
		
		if (leaseId.equals(leaseIdForProcessor)){
			return true;
		}else{
			throw new InfrastructureErrorException("When renewing, proposed lease ID '" + leaseIdForProcessor + "' is not adopted by Azure, returned lease ID is '" + leaseId + "'");
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#releaseLease(java.lang.String, java.lang.String)
	 */
	@Override
	public void releaseLease(String progressId, String processorId) throws InfrastructureErrorException, NotOwningLeaseException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#getProcessor(java.lang.String)
	 */
	@Override
	public String getProcessor(String progressId) throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#startTransaction(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.time.Instant, java.io.Serializable, java.lang.String)
	 */
	@Override
	public String startTransaction(String progressId, String processorId, String startPosition, String endPosition, Instant timeout,
			Serializable transaction, String transactionId) throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException,
			InfrastructureErrorException, TransactionTimeoutAfterLeaseExpirationException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#finishTransaction(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void finishTransaction(String progressId, String processorId, String transactionId, String endPosition)
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NotCurrentTransactionException,
			NotOwningLeaseException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#abortTransaction(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void abortTransaction(String progressId, String processorId, String transactionId, String endPosition)
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NotCurrentTransactionException,
			NotOwningLeaseException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#retryLastUnsuccessfulTransaction(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public ProgressTransaction retryLastUnsuccessfulTransaction(String progressId, String processorId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException, NotOwningLeaseException,
			TransactionTimeoutAfterLeaseExpirationException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#renewTransactionTimeout(java.lang.String, java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public void renewTransactionTimeout(String progressId, String processorId, String transactionId, Instant timeout)
			throws NotOwningTransactionException, InfrastructureErrorException, NotOwningLeaseException, NotCurrentTransactionException,
			TransactionTimeoutAfterLeaseExpirationException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#isTransactionSuccessful(java.lang.String, java.lang.String, java.time.Instant)
	 */
	@Override
	public boolean isTransactionSuccessful(String progressId, String transactionId, Instant beforeWhen) throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.txprogress.TransactionalProgress#getLastSuccessfulTransaction(java.lang.String)
	 */
	@Override
	public ProgressTransaction getLastSuccessfulTransaction(String progressId) throws InfrastructureErrorException {
		// TODO Auto-generated method stub
		return null;
	}


}
