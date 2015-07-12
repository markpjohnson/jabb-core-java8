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

import net.sf.jabb.txprogress.ProgressTransaction;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.ex.IllegalTransactionStateException;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.ex.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.txprogress.ex.NotCurrentTransactionException;
import net.sf.jabb.txprogress.ex.NotOwningLeaseException;
import net.sf.jabb.txprogress.ex.NotOwningTransactionException;
import net.sf.jabb.txprogress.ex.TransactionTimeoutAfterLeaseExpirationException;

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
	
	protected TransactionalProgressTest(){
		tracker = createTracker();
	}

	abstract protected TransactionalProgress createTracker();
	
	@Test
	public void test01NormalLease() throws InfrastructureErrorException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		assertTrue("renewLease(...) should succeed", tracker.renewLease(progressId, processorId, Duration.ofMinutes(3)));
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}

	@Test
	public void test02LeaseTimeout() throws InterruptedException, InfrastructureErrorException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMillis(600)));
		assertEquals("should still own the lease", processorId, tracker.getProcessor(progressId));
		
		Thread.sleep(610);
		
		assertEquals("should already have lost the lease", null, tracker.getProcessor(progressId));
		
		
		assertFalse("renewLease(...) should fail after time out", tracker.renewLease(progressId, processorId, Duration.ofMinutes(3)));
		assertFalse("releaseLeaseIfOwning(...) should return false after time out", tracker.releaseLeaseIfOwning(progressId, processorId));
		
		try {
			tracker.releaseLease(progressId, processorId);
			fail("releaseLease(...) should throw NotOwningLeaseException");
		} catch (NotOwningLeaseException e) {
		}
	}
	
	protected void testConcurrentLease(BiFunction<String, AtomicInteger, Boolean> body) throws InterruptedException{
		ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CONCURRENT_THREADS);
		AtomicBoolean runFlag = new AtomicBoolean(false);
		AtomicInteger acquiredLease = new AtomicInteger(0);
		AtomicReference<Exception> lastException = new AtomicReference<>(null);
		
		for (int i = 0; i < NUM_CONCURRENT_THREADS; i ++){
			threadPool.execute(()->{
				String processorId = UUID.randomUUID().toString();
				while(!runFlag.get()){}
				while(runFlag.get()){
					try{
						if(body.apply(processorId, acquiredLease)){
							break;
						}
					}catch(Exception e){
						lastException.set(e);
						break;
					}
				}
			});
		}
		
		Thread.sleep(500);
		runFlag.set(true);
		Thread.sleep(1000L*30);
		runFlag.set(false);
		
		threadPool.shutdown();
		threadPool.awaitTermination(1, TimeUnit.MINUTES);
		
		assertEquals("Number of successful lease acquisition should match", 2, acquiredLease.get());
		assertEquals("The last exception is " + lastException.get(), null, lastException.get());
	}
	
	@Test
	public void test03ConcurrentLeaseWithTimeout() throws InterruptedException{
		testConcurrentLease((processorId, acquiredLease)->{
			boolean acquired;
			try {
				acquired = tracker.acquireLease(progressId, processorId, Duration.ofSeconds(20));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (acquired){
				acquiredLease.incrementAndGet();
				return true;
			}else{
				return false;
			}
		});
	}

	@Test
	public void test04ConcurrentLeaseWithRelease() throws InterruptedException{
		testConcurrentLease((processorId, acquiredLease)->{
			boolean acquired;
			try {
				acquired = tracker.acquireLease(progressId, processorId, Duration.ofSeconds(2000));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (acquired){
				acquiredLease.incrementAndGet();
				Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
				try {
					tracker.releaseLeaseIfOwning(progressId, processorId);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return true;
			}else{
				return false;
			}
		});
	}

	@Test
	public void test05ConcurrentLeaseWithRenew() throws InterruptedException{
		testConcurrentLease((processorId, acquiredLease)->{
			boolean acquired;
			try {
				acquired = tracker.acquireLease(progressId, processorId, Duration.ofSeconds(3));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (acquired){
				acquiredLease.incrementAndGet();
				for (int i = 0; i < 9; i ++){
					Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
					try {
						tracker.renewLease(progressId, processorId, Duration.ofSeconds(3));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return true;
			}else{
				return false;
			}
		});
	}

	@Test
	public void test10TransactionSucceeded() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null);
		Instant duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		assertEquals(transactionId, tracker.getLastSuccessfulTransaction(progressId).getTransactionId());
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}
	
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
		ProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
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
		ProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
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
		ProgressTransaction transaction = tracker.retryLastUnsuccessfulTransaction(progressId, processorId, Duration.ofSeconds(1));
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



}
