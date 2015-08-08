package net.sf.jabb.seqtx.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.microsoft.azure.storage.CloudStorageAccount;

import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator.TransactionCounts;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.seqtx.mem.InMemSequentialTransactionsCoordinator;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureSequentialTransactionsCoordinatorTest extends SequentialTransactionsCoordinatorTest{

	public AzureSequentialTransactionsCoordinatorTest(){
		super();
		timeScale = 20;
	}
	
	
	@Override
	protected SequentialTransactionsCoordinator createCoordinator() throws InvalidKeyException, URISyntaxException {
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		SequentialTransactionsCoordinator tracker = new AzureSequentialTransactionsCoordinator(storageAccount, "TestTable");
		return tracker;
	}
	
	@Test
	public void test00ClearAll() throws InfrastructureErrorException{
		tracker.clearAll();
	}
	
	@Test(expected = IllegalEndPositionException.class)
	public void test01FinishWithNewEndAfterNextCreated() throws InfrastructureErrorException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException{
		tracker.clear(seriesId);
		
		// start a new one
		SequentialTransaction transaction = tracker.startTransaction(seriesId, processorId, Duration.ofMinutes(2), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		String previousId = transaction.getTransactionId();
		transaction.setTransactionId(null);
		transaction.setStartPosition("1");
		transaction.setEndPosition("1");
		transaction = tracker.startTransaction(seriesId, previousId, null, transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		String firstId = transaction.getTransactionId();
		
		// start another one:
		transaction = tracker.startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofMinutes(2)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(firstId, transaction.getTransactionId());
		assertEquals("1", transaction.getStartPosition());
		
		transaction.setTransactionId(null);
		transaction.setStartPosition("2");
		transaction.setEndPosition("2");
		transaction = tracker.startTransaction(seriesId, firstId, "1", transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		String secondId = transaction.getTransactionId();
		
		// finish the first one with another end position
		tracker.finishTransaction(seriesId, processorId, firstId, "3");
		fail("non-last transaction should not be able to finish with a new end position");
	}
	
	@Test
	public void test02CreateAfterLastFinishedWithNewEnd() throws InfrastructureErrorException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException{
		tracker.clear(seriesId);
		
		// start a new one
		SequentialTransaction transaction = tracker.startTransaction(seriesId, processorId, Duration.ofMinutes(2), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		String previousId = transaction.getTransactionId();
		transaction.setTransactionId(null);
		transaction.setStartPosition("1");
		transaction.setEndPosition("1");
		transaction = tracker.startTransaction(seriesId, previousId, "1", transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		String firstId = transaction.getTransactionId();
		
		// prepare to start another one:
		transaction = tracker.startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofMinutes(2)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(firstId, transaction.getTransactionId());
		assertEquals("1", transaction.getStartPosition());

		
		// finish the first one with another end position
		tracker.finishTransaction(seriesId, processorId, firstId, "3");
		
		// actually start another one
		transaction.setTransactionId(null);
		transaction.setStartPosition("2");
		transaction.setEndPosition("2");
		transaction = tracker.startTransaction(seriesId, firstId, "1", transaction, 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());

	}

}
