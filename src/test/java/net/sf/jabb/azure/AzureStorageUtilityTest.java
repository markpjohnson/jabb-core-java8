/**
 * 
 */
package net.sf.jabb.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.jabb.util.retry.AttemptStrategy;
import net.sf.jabb.util.retry.StopStrategies;
import net.sf.jabb.util.retry.TooManyAttemptsException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;

/**
 * @author James Hu
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( { CloudTableClient.class, CloudTable.class, CloudQueueClient.class, CloudQueue.class })
public class AzureStorageUtilityTest {

	@Test
	public void testRetryTableCreationWithOnePositive() throws URISyntaxException, StorageException {
		String tableName = "TestTableName";
		
		CloudTableClient tableClient = Mockito.mock(CloudTableClient.class);
		CloudTable table = Mockito.mock(CloudTable.class);
		when(tableClient.getTableReference(tableName)).thenReturn(table);
		
		// one successful
		when(table.createIfNotExists()).thenReturn(true);
		boolean result = AzureStorageUtility.createIfNotExist(tableClient, tableName);
		verify(table, times(1)).createIfNotExists();
		assertTrue(result);
	}
	
	@Test
	public void testRetryTableCreationWithTwoFailedFollowedByOnePositive() throws URISyntaxException, StorageException {
		String tableName = "TestTableName";
		AtomicInteger count = new AtomicInteger();
		
		CloudTableClient tableClient = Mockito.mock(CloudTableClient.class);
		CloudTable table = Mockito.mock(CloudTable.class);
		when(tableClient.getTableReference(tableName)).thenReturn(table);
		
		// two failed followed by one successful
		when(table.createIfNotExists()).thenAnswer(i->{
			if (count.incrementAndGet() >= 3){
				return true;
			}else{
				StorageException e = new StorageException(StorageErrorCodeStrings.TABLE_BEING_DELETED, "", 409, null, null);
				throw e;
			}
		});
		boolean result = AzureStorageUtility.createIfNotExist(tableClient, tableName);
		verify(table, times(3)).createIfNotExists();
		assertTrue(result);
	}

	@Test
	public void testRetryTableCreationWithTwoFailedFollowedByOneNegative() throws URISyntaxException, StorageException {
		String tableName = "TestTableName";
		AtomicInteger count = new AtomicInteger();
		
		CloudTableClient tableClient = Mockito.mock(CloudTableClient.class);
		CloudTable table = Mockito.mock(CloudTable.class);
		when(tableClient.getTableReference(tableName)).thenReturn(table);
		
		// two failed followed by one successful
		when(table.createIfNotExists()).thenAnswer(i->{
			if (count.incrementAndGet() >= 3){
				return false;
			}else{
				StorageException e = new StorageException(StorageErrorCodeStrings.TABLE_BEING_DELETED, "", 409, null, null);
				throw e;
			}
		});
		boolean result = AzureStorageUtility.createIfNotExist(tableClient, tableName);
		verify(table, times(3)).createIfNotExists();
		assertFalse(result);
	}

	@Test
	public void testRetryTableCreationWithAllFailed() throws URISyntaxException, StorageException {
		String tableName = "TestTableName";
		
		CloudTableClient tableClient = Mockito.mock(CloudTableClient.class);
		CloudTable table = Mockito.mock(CloudTable.class);
		when(tableClient.getTableReference(tableName)).thenReturn(table);
		
		// two failed followed by one successful
		when(table.createIfNotExists()).thenThrow(new StorageException(StorageErrorCodeStrings.TABLE_BEING_DELETED, "", 409, null, null));
		try{
			AzureStorageUtility.createIfNotExist(tableClient, tableName, new AttemptStrategy().withStopStrategy(StopStrategies.stopAfterTotalAttempts(4)));
			fail("should have exception thrown");
		}catch(StorageException e){
			Throwable[] suppressed = e.getSuppressed();
			assertNotNull(suppressed);
			assertEquals(1, suppressed.length);
			assertEquals(TooManyAttemptsException.class, suppressed[0].getClass());
		}finally{
			verify(table, times(4)).createIfNotExists();
		}
	}

	@Test
	public void testRetryQueueCreationWithOnePositive() throws URISyntaxException, StorageException {
		String queueName = "TestQueueName";
		
		CloudQueueClient queueClient = Mockito.mock(CloudQueueClient.class);
		CloudQueue queue= Mockito.mock(CloudQueue.class);
		when(queueClient.getQueueReference(queueName)).thenReturn(queue);
		
		// one successful
		when(queue.createIfNotExists()).thenReturn(true);
		boolean result = AzureStorageUtility.createIfNotExist(queueClient, queueName);
		verify(queue, times(1)).createIfNotExists();
		assertTrue(result);
	}
	
	@Test
	public void testRetryQueueCreationWithTwoFailedFollowedByOnePositive() throws URISyntaxException, StorageException {
		String queueName = "TestQueueName";
		AtomicInteger count = new AtomicInteger();
		
		CloudQueueClient queueClient = Mockito.mock(CloudQueueClient.class);
		CloudQueue queue= Mockito.mock(CloudQueue.class);
		when(queueClient.getQueueReference(queueName)).thenReturn(queue);
		
		// two failed followed by one successful
		when(queue.createIfNotExists()).thenAnswer(i->{
			if (count.incrementAndGet() >= 3){
				return true;
			}else{
				StorageException e = new StorageException(StorageErrorCodeStrings.QUEUE_BEING_DELETED, "", 409, null, null);
				throw e;
			}
		});
		boolean result = AzureStorageUtility.createIfNotExist(queueClient, queueName);
		verify(queue, times(3)).createIfNotExists();
		assertTrue(result);
	}

}
