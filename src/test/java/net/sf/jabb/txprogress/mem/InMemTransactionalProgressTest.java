package net.sf.jabb.txprogress.mem;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import net.sf.jabb.txprogress.BasicProgressTransaction;
import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.TransactionalProgressTest;
import net.sf.jabb.txprogress.ex.InfrastructureErrorException;
import net.sf.jabb.txprogress.mem.InMemTransactionalProgress;
import net.sf.jabb.txprogress.mem.InMemTransactionalProgress.TransactionCounts;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InMemTransactionalProgressTest extends TransactionalProgressTest{

	@Override
	protected TransactionalProgress createTracker() {
		TransactionalProgress tracker = new InMemTransactionalProgress();
		return tracker;
	}
	
	@Test
	public void test00CompactEmpty(){
		LinkedList<BasicProgressTransaction> transactions = new LinkedList<>();
		InMemTransactionalProgress tracker = (InMemTransactionalProgress)createTracker();

		tracker.compact(transactions);
		assertEquals(0, transactions.size());

		TransactionCounts counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);
	}

	@Test
	public void test00CompactMore(){
		LinkedList<BasicProgressTransaction> transactions = new LinkedList<>();
		InMemTransactionalProgress tracker = (InMemTransactionalProgress)createTracker();

		BasicProgressTransaction t1 = new BasicProgressTransaction("transaction01", "processor01", "001", "010", Instant.now().plusSeconds(3600), null);
		transactions.add(t1);
		TransactionCounts counts = tracker.compactAndGetCounts(transactions);
		assertEquals(1, transactions.size());	// t1
		assertEquals(0, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);
		
		t1.finish();
		tracker.compact(transactions);
		assertEquals(1, transactions.size());	// t1
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		
		BasicProgressTransaction t2 = new BasicProgressTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		transactions.add(t2);
		tracker.compact(transactions);
		assertEquals(2, transactions.size());	// t1, t2
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		
		t2.finish();
		tracker.compact(transactions);
		assertEquals(1, transactions.size());
		assertEquals(t2, transactions.getFirst());	// t2
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		
		transactions.add(t1);
		transactions.add(t2);
		transactions.add(t1);		// t2, t1, t2, t1
		tracker.compact(transactions);
		assertEquals(1, transactions.size());
		assertEquals(t1, transactions.getFirst());	// t1
		
		BasicProgressTransaction t3 = new BasicProgressTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		BasicProgressTransaction t4 = new BasicProgressTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		transactions.add(t2);
		transactions.add(t3);
		transactions.add(t4);	// t1, t2, t3, t4
		tracker.compact(transactions);			// t2, t3, t4
		assertEquals(3, transactions.size());
		assertEquals(t2, transactions.getFirst());	// t2, t3, t4
		assertEquals(t4, transactions.getLast());
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(2, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		
		t4.finish();
		tracker.compact(transactions);
		assertEquals(3, transactions.size());
		assertEquals(t4, transactions.getLast());	// t2, t3, t4
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		t3.finish();
		tracker.compact(transactions);			// t4
		assertEquals(1, transactions.size());
		assertEquals(t4, transactions.getFirst());	// t4
		
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);
	}
	
	@Test
	public void test00CompactTimedOutAndRetry(){
		LinkedList<BasicProgressTransaction> transactions = new LinkedList<>();
		InMemTransactionalProgress tracker = (InMemTransactionalProgress)createTracker();
		
		BasicProgressTransaction t1 = new BasicProgressTransaction("transaction01", "processor01", "001", "010", Instant.now().plusSeconds(-1), null);
		transactions.add(t1);
		TransactionCounts counts = tracker.compactAndGetCounts(transactions);
		assertEquals(1, transactions.size());	// t1
		assertEquals(1, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		t1.retry("processor02", Instant.now().plusSeconds(3600));
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(1, transactions.size());	// t1
		assertEquals(0, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(1, counts.retryingCount);
		
		BasicProgressTransaction t2 = new BasicProgressTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		transactions.add(t2);
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(2, transactions.size());	// t1, t2
		assertEquals(0, counts.failedCount);
		assertEquals(2, counts.inProgressCount);
		assertEquals(1, counts.retryingCount);

		t2.abort();
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(2, transactions.size());	// t1, t2
		assertEquals(1, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(1, counts.retryingCount);
		
	}

	@Test
	public void test00ClearAll() throws InfrastructureErrorException{
		tracker.clearAll();
	}
}
