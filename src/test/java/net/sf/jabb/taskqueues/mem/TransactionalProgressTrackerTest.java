/**
 * 
 */
package net.sf.jabb.taskqueues.mem;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;

import net.sf.jabb.transprogtracker.LastTransactionIsNotSuccessfulException;
import net.sf.jabb.transprogtracker.NotOwningLeaseException;
import net.sf.jabb.transprogtracker.NotOwningTransactionException;
import net.sf.jabb.transprogtracker.TransactionalProgressTracker;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Test;

/**
 * The base test
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class TransactionalProgressTrackerTest {
	
	protected TransactionalProgressTracker tracker;
	protected String progressId = "Test progress Id 1";
	protected String processorId = "Test processor 1";
	
	protected TransactionalProgressTrackerTest(){
		tracker = createTracker();
	}

	abstract protected TransactionalProgressTracker createTracker();
	
	@Test
	public void test01NormalLease(){
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		assertTrue("renewLease(...) should succeed", tracker.renewLease(progressId, processorId, Duration.ofMinutes(3)));
		assertTrue("releaseLease(...) should succeed", tracker.releaseLease(progressId, processorId));
	}

	@Test
	public void test02LeaseTimeout() throws InterruptedException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMillis(600)));
		assertEquals("should still own the lease", processorId, tracker.getProcessor(progressId));
		
		Thread.sleep(600);
		
		assertEquals("should already have lost the lease", null, tracker.getProcessor(progressId));
		
		
		assertFalse("renewLease(...) should fail after time out", tracker.renewLease(progressId, processorId, Duration.ofMinutes(3)));
		assertTrue("releaseLease(...) should succeed after time out", tracker.releaseLease(progressId, processorId));
	}

	@Test
	public void test10Transaction() throws LastTransactionIsNotSuccessfulException, NotOwningLeaseException, NotOwningTransactionException{
		assertTrue("acquireLease(...) should succeed", tracker.acquireLease(progressId, processorId, Duration.ofMinutes(1)));
		
		String transactionId = tracker.startTransaction(progressId, processorId, "001", "010", Duration.ofSeconds(1), null);
		Instant duringTransaction = Instant.now();
		tracker.finishTransaction(progressId, processorId, transactionId);
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, "non-exist", duringTransaction));
		assertTrue(tracker.isTransactionSuccessful(progressId, transactionId, Instant.now().plusSeconds(36000)));
		
		assertTrue("releaseLease(...) should succeed", tracker.releaseLease(progressId, processorId));
	}


}
