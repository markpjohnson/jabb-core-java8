package net.sf.jabb.taskq.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import net.sf.jabb.taskq.ScheduledTaskQueues;

import org.junit.Test;

import com.google.common.base.Throwables;
import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * The test case with 3 characters from task Ids in partition keys
 * @author James Hu
 *
 */
public class AzureScheduledTaskQueuesIntegrationTest3 extends AzureScheduledTaskQueuesIntegrationTest {

	static protected AzureScheduledTaskQueues createAzureScheduledTaskQueues()  throws InvalidKeyException, URISyntaxException{
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		AzureScheduledTaskQueues taskq = new AzureScheduledTaskQueues(storageAccount, "TestTable", 3);
		return taskq;
		
	}
	
	//@Override
	protected ScheduledTaskQueues createScheduledTaskQueues() throws InvalidKeyException, URISyntaxException {
		return createAzureScheduledTaskQueues();
	}

}
