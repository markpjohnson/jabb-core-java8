package net.sf.jabb.seqtx.mem;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.LinkedList;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.mem.InMemSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.mem.InMemSequentialTransactionsCoordinator.TransactionCounts;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InMemSequentialTransactionsCoordinatorTest extends SequentialTransactionsCoordinatorTest{

	@Override
	protected SequentialTransactionsCoordinator createTracker() {
		SequentialTransactionsCoordinator tracker = new InMemSequentialTransactionsCoordinator();
		return tracker;
	}
	
	@Test
	public void test00CompactEmpty(){
		LinkedList<SimpleSequentialTransaction> transactions = new LinkedList<>();
		InMemSequentialTransactionsCoordinator tracker = (InMemSequentialTransactionsCoordinator)createTracker();

		tracker.compact(transactions);
		assertEquals(0, transactions.size());

		TransactionCounts counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);
	}

	@Test
	public void test00CompactMore(){
		LinkedList<SimpleSequentialTransaction> transactions = new LinkedList<>();
		InMemSequentialTransactionsCoordinator tracker = (InMemSequentialTransactionsCoordinator)createTracker();

		SimpleSequentialTransaction t1 = new SimpleSequentialTransaction("transaction01", "processor01", "001", "010", Instant.now().plusSeconds(3600), null);
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

		
		SimpleSequentialTransaction t2 = new SimpleSequentialTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
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
		
		SimpleSequentialTransaction t3 = new SimpleSequentialTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		SimpleSequentialTransaction t4 = new SimpleSequentialTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
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
		LinkedList<SimpleSequentialTransaction> transactions = new LinkedList<>();
		InMemSequentialTransactionsCoordinator tracker = (InMemSequentialTransactionsCoordinator)createTracker();
		
		SimpleSequentialTransaction t1 = new SimpleSequentialTransaction("transaction01", "processor01", "001", "010", Instant.now().plusSeconds(-1), null);
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
		
		SimpleSequentialTransaction t2 = new SimpleSequentialTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
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
	public void test00CompactOpenTransactions(){
		LinkedList<SimpleSequentialTransaction> transactions = new LinkedList<>();
		InMemSequentialTransactionsCoordinator tracker = (InMemSequentialTransactionsCoordinator)createTracker();
		
		SimpleSequentialTransaction t1 = new SimpleSequentialTransaction("transaction01", "processor01", "001", null, Instant.now().plusSeconds(-1), null);
		transactions.add(t1);
		TransactionCounts counts = tracker.compactAndGetCounts(transactions);
		assertEquals(0, transactions.size());	// t1
		assertEquals(0, counts.failedCount);
		assertEquals(0, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

		SimpleSequentialTransaction t2 = new SimpleSequentialTransaction("transaction01", "processor01", "011", "020", Instant.now().plusSeconds(3600), null);
		transactions.add(t2);
		transactions.add(t1);
		counts = tracker.compactAndGetCounts(transactions);
		assertEquals(1, transactions.size());	// t2
		assertEquals(0, counts.failedCount);
		assertEquals(1, counts.inProgressCount);
		assertEquals(0, counts.retryingCount);

	}


	@Test
	public void test00ClearAll() throws InfrastructureErrorException{
		tracker.clearAll();
	}
}
