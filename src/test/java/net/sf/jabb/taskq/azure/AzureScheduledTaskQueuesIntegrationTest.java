/**
 * 
 */
package net.sf.jabb.taskq.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.azure.AzureSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.taskq.ReadOnlyScheduledTask;
import net.sf.jabb.taskq.ScheduledTaskQueues;
import net.sf.jabb.taskq.ex.NoSuchTaskException;
import net.sf.jabb.taskq.ex.NotOwningTaskException;
import net.sf.jabb.taskq.ex.TaskQueueStorageInfrastructureException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureScheduledTaskQueuesIntegrationTest {
	static private final Logger logger = LoggerFactory.getLogger(AzureScheduledTaskQueuesIntegrationTest.class);

	static private final String Q1 = "TestQueue1";
	static private final String Q2 = "TestQueue2";
	
	static private final String P1 = "Processor 1";
	static private final String P2 = "Processor 2";
	
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
	public void test10ClearQueue() throws TaskQueueStorageInfrastructureException{
		taskq.clear(Q1);
		taskq.clear(Q2);
		taskq.clearAll();
	}
	
	@Test
	public void test11CreateTask() 
			throws TaskQueueStorageInfrastructureException, NotOwningTaskException, NoSuchTaskException, InterruptedException{
		taskq.clear(Q1);
		Instant expectedExecutionTime = Instant.now();
		String id = taskq.put(Q1, "task1");
		assertNotNull(id);
		
		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());
		assertEquals(id, tasks.get(0).getTaskId());
		assertEquals("task1", tasks.get(0).getDetail());
		assertEquals(1, tasks.get(0).getAttempts());
		assertEquals(expectedExecutionTime.toEpochMilli(), tasks.get(0).getExpectedExecutionTime().toEpochMilli());
		
		taskq.abort(id, P1);
		
		String id2 = taskq.put(Q1, "task2", Duration.ofSeconds(10));
		assertNotNull(id2);
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());
		assertEquals(id, tasks.get(0).getTaskId());
		assertEquals("task1", tasks.get(0).getDetail());
		assertEquals(2, tasks.get(0).getAttempts());
		assertEquals(expectedExecutionTime.toEpochMilli(), tasks.get(0).getExpectedExecutionTime().toEpochMilli());
		
		Thread.sleep(1000L*11);
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());
		assertEquals(1, tasks.stream().filter(t->t.getTaskId().equals(id2) && "task2".equals(t.getDetail())).count());
		
		for (ReadOnlyScheduledTask t: tasks){
			taskq.finish(t.getTaskId(), P1);
		}
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(0, tasks.size());
	}
	
	@Test
	public void test13CreateTaskWithPredecessor() throws TaskQueueStorageInfrastructureException, NotOwningTaskException, NoSuchTaskException{
		taskq.clear(Q1);
		taskq.clear(Q2);
		
		String id1 = taskq.put(Q1, "task1");
		assertNotNull(id1);
		
		String id2 = taskq.put(Q1, "task2");
		assertNotNull(id2);
		
		String id3 = taskq.put(Q1, "task3", id1);
		assertNotNull(id3);

		String id4 = taskq.put(Q2, "task4", id1);
		assertNotNull(id4);
		
		String id5 = taskq.put(Q2, "task5", id1);
		assertNotNull(id5);
		
		String id6 = taskq.put(Q2, "task6", id1);
		assertNotNull(id6);
		
		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());

		tasks = taskq.get(Q2, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(0, tasks.size());
		
		taskq.finish(id1, P1);
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());
		assertEquals(id3, tasks.get(0).getTaskId());
		assertEquals("task3", tasks.get(0).getDetail());

		tasks = taskq.get(Q2, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(3, tasks.size());
		//assertEquals(id4, tasks.get(0).getTaskId());
		//assertEquals("task4", tasks.get(0).getDetail());
		
	}
	
	@Test
	public void test14GetTasks() throws TaskQueueStorageInfrastructureException, InvalidKeyException, URISyntaxException{
		taskq.clear(Q1);
		taskq.clear(Q2);

		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(0, tasks.size());
		
		for (int i = 0; i < 56; i ++){
			createScheduledTaskQueues().put(Q1, "task " + i);
		}
		
		for (int i = 0; i < 5; i ++){
			tasks = taskq.get(Q1, 10, P1, Duration.ofMinutes(1));
			assertNotNull(tasks);
			assertEquals(10, tasks.size());
		}
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofMinutes(1));
		assertNotNull(tasks);
		assertEquals(6, tasks.size());
	}
	
	@Test
	public void test15FinishTask() throws TaskQueueStorageInfrastructureException, NotOwningTaskException, NoSuchTaskException{
		taskq.clear(Q1);
		
		String id1 = taskq.put(Q1, "task1");
		assertNotNull(id1);
		
		String id2 = taskq.put(Q1, "task2");
		assertNotNull(id2);
		
		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());

		// finish by another processor
		try{
			taskq.finish(tasks.get(0).getTaskId(), P2);
			fail("Should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){};
		
		// finish normally then get
		taskq.finish(tasks.get(0).getTaskId(), P1);
		taskq.abort(tasks.get(1).getTaskId(), P1);
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());

		String id = tasks.get(0).getTaskId();
		taskq.finish(id, P1);
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(10));
		assertNotNull(tasks);
		assertEquals(0, tasks.size());

		
		// finish already finished
		try{
			taskq.finish(id, P1);
			fail("Should throw NoSuchTaskException");
		}catch(NoSuchTaskException e){}
	}
	
	@Test
	public void test16TimeoutTask() throws TaskQueueStorageInfrastructureException, InterruptedException, NotOwningTaskException, NoSuchTaskException{
		taskq.clear(Q1);
		
		String id1 = taskq.put(Q1, "task1");
		assertNotNull(id1);
		
		String id2 = taskq.put(Q1, "task2");
		assertNotNull(id2);
		
		
		// timeout then get
		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(0, tasks.size());
		
		Thread.sleep(1000L*6);
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());

		Thread.sleep(1000L*6);

		// timeout then finish
		try{
			taskq.finish(id1, P1);
			fail("should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){}
		
		// timeout then abort
		try{
			taskq.abort(id1, P1);
			fail("should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){}
		
		// timeout then renew
		try{
			taskq.renewTimeout(id1, P1, Duration.ofSeconds(10));
			fail("should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){}
		
		// renew timeout
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(2, tasks.size());
		taskq.renewTimeout(id1, P1, Duration.ofSeconds(10));
		Thread.sleep(1000L*6);
		
		taskq.finish(id1, P1);
		
		try{
			taskq.finish(id2, P1);
			fail("should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){}

	}
	
	@Test
	public void test17AbortTask() throws TaskQueueStorageInfrastructureException, NotOwningTaskException, NoSuchTaskException{
		taskq.clear(Q1);
		
		String id1 = taskq.put(Q1, "task1");
		assertNotNull(id1);

		// abort normally then retry
		List<ReadOnlyScheduledTask> tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());
		
		assertEquals(0, taskq.get(Q1, 10, P1, Duration.ofSeconds(5)).size());
		taskq.abort(tasks.get(0).getTaskId(), P1);
		
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());

		
		taskq.finish(id1, P1);
		// abort finished
		try{
			taskq.abort(id1, P1);
			fail("should throw NoSuchTaskException");
		}catch(NoSuchTaskException e){};
		
		// abort already aborted
		id1 = taskq.put(Q1, "task1");
		assertNotNull(id1);
		tasks = taskq.get(Q1, 10, P1, Duration.ofSeconds(5));
		assertNotNull(tasks);
		assertEquals(1, tasks.size());
		taskq.abort(id1, P1);
		try{
			taskq.abort(id1, P1);
			fail("should throw NotOwningTaskException");
		}catch(NotOwningTaskException e){};
	}

}
