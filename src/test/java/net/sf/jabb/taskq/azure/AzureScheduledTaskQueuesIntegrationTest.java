/**
 * 
 */
package net.sf.jabb.taskq.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.azure.AzureSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.taskq.ScheduledTaskQueues;
import net.sf.jabb.taskq.ex.TaskQueueStorageInfrastructureException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * @author James Hu
 *
 */
public class AzureScheduledTaskQueuesIntegrationTest {
	static private final Logger logger = LoggerFactory.getLogger(AzureScheduledTaskQueuesIntegrationTest.class);

	protected ScheduledTaskQueues taskq;
	
	static protected AzureScheduledTaskQueues createAzureScheduledTaskQueues()  throws InvalidKeyException, URISyntaxException{
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		AzureScheduledTaskQueues taskq = new AzureScheduledTaskQueues(storageAccount, "TestTable");
		return taskq;
		
	}
	
	public AzureScheduledTaskQueuesIntegrationTest(){
		try {
			taskq = createScheduledTaskQueues();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
	
	//@Override
	protected ScheduledTaskQueues createScheduledTaskQueues() throws InvalidKeyException, URISyntaxException {
		return createAzureScheduledTaskQueues();
	}
	
	@BeforeClass
	@AfterClass
	static public void clearAll() throws  InvalidKeyException, URISyntaxException, TaskQueueStorageInfrastructureException{
		createAzureScheduledTaskQueues().clearAll();
	}
	
	@Test
	public void test10ClearQueue(){
		
	}
	
	@Test
	public void test11CreateTask(){
		
	}
	
	@Test
	public void test13CreateTaskWithPredecessor(){
		
	}
	
	@Test
	public void test14GetTasks(){
		
	}
	
	@Test
	public void test15FinishTask(){
		// finish normally then get
		
		// finish already finished
	}
	
	@Test
	public void test16TimeoutTask(){
		
		// timeout then get
		
		// timeout then finish
		
		// timeout then abort
	}
	
	@Test
	public void test17AbortTask(){
		// abort normally then retry
		
		// abort finished
		
		// abort already aborted
		
	}

}
