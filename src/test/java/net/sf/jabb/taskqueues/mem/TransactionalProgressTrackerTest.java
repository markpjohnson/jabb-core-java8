/**
 * 
 */
package net.sf.jabb.taskqueues.mem;

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

import net.sf.jabb.transprogtracker.TransactionalProgressTracker;
import net.sf.jabb.transprogtracker.ex.IllegalTransactionStateException;
import net.sf.jabb.transprogtracker.ex.InfrastructureErrorException;
import net.sf.jabb.transprogtracker.ex.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.transprogtracker.ex.NotCurrentTransactionException;
import net.sf.jabb.transprogtracker.ex.NotOwningLeaseException;
import net.sf.jabb.transprogtracker.ex.NotOwningTransactionException;
import net.sf.jabb.transprogtracker.ex.TransactionTimeoutAfterLeaseExpirationException;

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
public abstract class TransactionalProgressTrackerTest {
	protected final int NUM_CONCURRENT_THREADS = 20;
	
	protected TransactionalProgressTracker tracker;
	protected String progressId = "Test progress Id 1";
	protected String processorId = "Test processor 1";
	
	protected TransactionalProgressTrackerTest(){
		tracker = createTracker();
	}

	abstract protected TransactionalProgressTracker createTracker();
	
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
		
		Thread.sleep(600);
		
		assertEquals("should already have lost the lease", null, tracker.getProcessor(progressId));
		
		
		assertFalse("renewLease(...) should fail after time out", tracker.renewLease(progressId, processorId, Duration.ofMinutes(3)));
		assertTrue("releaseLease(...) should succeed after time out", tracker.releaseLeaseIfOwning(progressId, processorId));
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
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}

	@Test
	public void test11TransactionTimedOut() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException, InterruptedException, InfrastructureErrorException, IllegalTransactionStateException, NotCurrentTransactionException, TransactionTimeoutAfterLeaseExpirationException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null);
		Instant duringTransaction = Instant.now();
		Thread.sleep(1500L);
		
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLeaseIfOwning(progressId, processorId));
	}


}
