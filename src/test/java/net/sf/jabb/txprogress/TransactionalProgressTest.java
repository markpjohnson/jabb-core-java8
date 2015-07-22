/**
 * 
 */
package net.sf.jabb.txprogress;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import net.sf.jabb.txprogress.ReadOnlyProgressTransaction;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.ex.DuplicatedTransactionIdException;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.NoSuchTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;

import org.jgroups.util.UUID;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;


/**
 * The base test
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class TransactionalProgressTest {
	protected final int NUM_CONCURRENT_THREADS = 20;
	
	protected TransactionalProgress tracker;
	protected String progressId = "Test progress Id 1";
	protected String processorId = "Test processor 1";
	protected String transactionDetail = "This is the transaction detail";
	
	protected TransactionalProgressTest(){
		tracker = createTracker();
	}

	abstract protected TransactionalProgress createTracker();
	
	@Test
	public void test10StartTransactions() throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException{
		String lastId;
		// empty
		ProgressTransaction transaction = tracker.startTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertNull(transaction.getTransactionId());
		assertNull(transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setTransaction(transactionDetail);
		
		transaction = tracker.startTransaction(progressId, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(ProgressTransactionState.IN_PROGRESS, transaction.getState());
		assertNotNull(transaction.getTransactionId());
		assertEquals("001", transaction.getStartPosition());
		assertEquals("010", transaction.getEndPosition());
		assertEquals(transactionDetail, transaction.getTransaction());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(1, transaction.getAttempts());
		assertEquals(1, tracker.getRecentTransactions(progressId).size());
		assertEquals(ProgressTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(0).getState());
	
		lastId = transaction.getTransactionId();
		transaction = tracker.startTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);	// in-progress
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setTransaction(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120));
		
		transaction = tracker.startTransaction(progressId, "alksdjflksdj", transaction, 5, 5);  // will get a skeleton
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());

		transaction.setTransactionId(null);
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setTransaction(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120));

		transaction = tracker.startTransaction(progressId, lastId, transaction, 5, 5); // in-progress, in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(ProgressTransactionState.IN_PROGRESS, transaction.getState());
		assertNotNull(transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("020", transaction.getEndPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(1, transaction.getAttempts());
		assertEquals(2, tracker.getRecentTransactions(progressId).size());
		assertEquals(ProgressTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(0).getState());
		assertEquals(ProgressTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(1).getState());
		
		lastId = transaction.getTransactionId();
		tracker.abortTransaction(progressId, processorId, lastId);	// in-progress, aborted
		
		transaction = tracker.startTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);	// in-progress, in-progress(retry)
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("020", transaction.getEndPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(2, transaction.getAttempts());
		assertEquals(2, tracker.getRecentTransactions(progressId).size());


	}
	
	/*
	@Test
	public void test11TransactionFailed() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InterruptedException, InfrastructureErrorException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException, IllegalTransactionStateException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		// abort
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null);
		Instant duringTransaction = Instant.now();
		
		tracker.abortTransaction(progressId, processorId, transactionId, "009");
			
		assertFalse(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertFalse(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertNull(tracker.getLastSuccessfulTransaction(progressId));
		
		// retry
		ReadOnlyProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
		assertNotNull("timed out transaction should be available for retry", transaction);
		assertEquals("timed out transaction should be available for retry", transactionId, transaction.getTransactionId());
		assertEquals("001", transaction.getStartPosition());
		assertEquals("009", transaction.getEndPosition());
		
		duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertEquals(transactionId, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}


	@Test
	public void test11TransactionTimedOut() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InterruptedException, InfrastructureErrorException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException, IllegalTransactionStateException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		// time out
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null);
		Instant duringTransaction = Instant.now();
		Thread.sleep(1500L);
		
		try{
			tracker.finishTransaction(progressId, processorId, transactionId);
			fail("finishTransaction(...) should fail after time out");
		}catch(IllegalTransactionStateException e){
			
		}
		assertFalse(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertFalse(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertNull(tracker.getLastSuccessfulTransaction(progressId));
		
		// retry
		ReadOnlyProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
		assertNotNull("timed out transaction should be available for retry", transaction);
		assertEquals("timed out transaction should be available for retry", transactionId, transaction.getTransactionId());
		assertEquals("001", transaction.getStartPosition());
		assertEquals("010", transaction.getEndPosition());
		
		duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertEquals(transactionId, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}

	@Test
	public void test11TransactionRenew() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InterruptedException, InfrastructureErrorException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException, IllegalTransactionStateException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		// time out
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofMillis(500), null);
		tracker.renewTransactionTimeout(progressId, processorId, transactionId, Duration.ofSeconds(1));
		Instant duringTransaction = Instant.now();
		Thread.sleep(600L);
		
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertEquals(transactionId, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}


	@Test
	public void test12MultipleTransactions() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InterruptedException, InfrastructureErrorException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException, IllegalTransactionStateException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		String id1 = "transactionId1";
		String id2 = "transactionId2";
		String id3 = "transactionId3";
		String id4 = "transactionId4";
		
		// too long
		try{
			tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofMinutes(2), null, id1);
			fail("should not be able to start a transaction longer than lease period");
		}catch(TransactionTimeoutAfterLeaseExpirationException e){
		}
		assertNull(tracker.getLastSuccessfulTransaction(progressId));
		
		// succeeded
		tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null, id1);
		Instant duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, id1);
		assertTrue(tracker.isTransactionSuccessful(progressId, id1));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().plusSeconds(36000)));
		assertEquals(id1, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		// failed
		tracker.startTransaction(progressId, processorId, "011", "020", Duration.ofSeconds(1), null, id2);
		duringTransaction = Instant.now();
		tracker.abortTransaction(progressId, processorId, id2, "015");
		assertFalse(tracker.isTransactionSuccessful(progressId, id2));
		assertFalse(tracker.isTransactionSuccessful(progressId, id2, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertFalse(tracker.isTransactionSuccessful(progressId, id2, Instant.now().plusSeconds(36000)));
		
		assertTrue(tracker.isTransactionSuccessful(progressId, id1));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().plusSeconds(36000)));
		assertEquals(id1, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		
		// start new should fail
		try{
			tracker.startTransaction(progressId, processorId, "021", "030", Duration.ofSeconds(1), null, id3);
			fail("starting a new transaction while the last one is not successful should fail");
		}catch(LastTransactionIsNotSuccessfulException e){
		}
		assertEquals(id1, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());
		
		// retry failed
		ReadOnlyProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
		assertNotNull("timed out transaction should be available for retry", transaction);
		assertEquals("timed out transaction should be available for retry", id2, transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("015", transaction.getEndPosition());
		
		duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, id2);
		assertTrue(tracker.isTransactionSuccessful(progressId, id2));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, Instant.now().plusSeconds(36000)));
		assertEquals(id2, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		assertTrue(tracker.isTransactionSuccessful(progressId, id1));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().plusSeconds(36000)));

		// succeeded
		tracker.startTransaction(progressId, processorId, "016", "020", Duration.ofSeconds(1), null, id3);
		duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, id3);
		assertTrue(tracker.isTransactionSuccessful(progressId, id3));
		assertTrue(tracker.isTransactionSuccessful(progressId, id3, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist"));
		assertTrue(tracker.isTransactionSuccessful(progressId, id3, Instant.now().plusSeconds(36000)));
		assertEquals(id3, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());

		assertTrue(tracker.isTransactionSuccessful(progressId, id2));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, Instant.now().plusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().plusSeconds(36000)));
		
		
		// time out
		tracker.startTransaction(progressId, processorId, "021", "030", Duration.ofMillis(100), null, id4);
		Thread.sleep(110L);
		
		assertFalse(tracker.isTransactionSuccessful(progressId, id4, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist"));
		assertFalse(tracker.isTransactionSuccessful(progressId, id4, Instant.now().plusSeconds(36000)));
		assertEquals(id3, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());
		
		assertTrue(tracker.isTransactionSuccessful(progressId, id3));
		assertTrue(tracker.isTransactionSuccessful(progressId, id3, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id3, Instant.now().plusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id2, Instant.now().plusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().minusSeconds(36000)));
		assertTrue(tracker.isTransactionSuccessful(progressId, id1, Instant.now().plusSeconds(36000)));
		
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}

*/

}
