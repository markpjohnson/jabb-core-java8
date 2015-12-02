/**
 * 
 */
package net.sf.jabb.seqtx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.util.col.PutIfAbsentMap;
import net.sf.jabb.util.stat.BasicFrequencyCounter;
import net.sf.jabb.util.text.DurationFormatter;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;


/**
 * The base test
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class SequentialTransactionsCoordinatorTest {
	static private final Logger logger = LoggerFactory.getLogger(SequentialTransactionsCoordinatorTest.class);

	protected final int NUM_PROCESSORS = 20;
	protected final int MAX_IN_PROGRESS_TRANSACTIONS = 15;
	protected final int MAX_RETRYING_TRANSACTIONS = 10;
	protected final int START_POSITION = 1;
	protected final int END_POSITION = 500;
	protected final Duration BASE_TIMEOUT_DURATION = Duration.ofMillis(500);
	
	protected SequentialTransactionsCoordinator tracker;
	protected String seriesId = "Test progress Id 1";
	protected String processorId = "Test processor 1";
	protected String transactionDetail = "This is the transaction detail";
	
	protected int timeScale = 1;
	
	protected SequentialTransactionsCoordinatorTest(){
		try {
			tracker = createCoordinator();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

	abstract protected SequentialTransactionsCoordinator createCoordinator() throws Exception;
	
	protected SequentialTransactionsCoordinator createPerProcessorCoordinator(){
		try {
			return createCoordinator();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
	
	@Test
	public void test09ClearTransactions() throws TransactionStorageInfrastructureException{
		tracker.clear(seriesId);
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(seriesId);
		assertNotNull(transactions);
		assertEquals(0, transactions.size());

	}
	
	@Test
	public void test10StartTransactions() throws NotOwningTransactionException, TransactionStorageInfrastructureException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException{
		tracker.clear(seriesId);
		
		String lastId;
		// empty
		SequentialTransaction transaction = createPerProcessorCoordinator().startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNull(transaction);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertNull(transaction.getTransactionId());
		assertNull(transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, null, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertNotNull(transaction.getTransactionId());
		assertEquals("001", transaction.getStartPosition());
		assertEquals("010", transaction.getEndPosition());
		assertEquals(transactionDetail, transaction.getDetail());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(1, transaction.getAttempts());
		assertEquals(1, tracker.getRecentTransactions(seriesId).size());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(seriesId).get(0).getState());
	
		lastId = transaction.getTransactionId();
		transaction = createPerProcessorCoordinator().startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);	// in-progress
		assertNull(transaction);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);	// in-progress
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120*timeScale));
		
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, "alksdjflksdj", "xyz", transaction, 5, 5);  // will get a skeleton
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());

		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120*timeScale));

		try{
			createPerProcessorCoordinator().startTransaction(seriesId, lastId, "010", transaction, 5, 5);
			fail("should throw DuplicatedTransactionIdException");
		}catch(DuplicatedTransactionIdException e){
			// ignore
		}
		transaction.setTransactionId(null);
		transaction = tracker.startTransaction(seriesId, lastId, "010", transaction, 5, 5); // in-progress, in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertNotNull(transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("020", transaction.getEndPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(1, transaction.getAttempts());
		assertEquals(2, tracker.getRecentTransactions(seriesId).size());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(seriesId).get(0).getState());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(seriesId).get(1).getState());
		
		assertNull(tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120), 1, 1));  // can't allow too many in progress
		
		lastId = transaction.getTransactionId();
		createPerProcessorCoordinator().abortTransaction(seriesId, processorId, lastId);	// in-progress, aborted
		
		transaction = createPerProcessorCoordinator().startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);	// in-progress, in-progress(retry)
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("020", transaction.getEndPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(2, transaction.getAttempts());
		assertEquals(2, tracker.getRecentTransactions(seriesId).size());

		assertNull(tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120), 1, 1));  // can't allow too many retry in progress
	}
	
	@Test
	public void test11InProgressTransactions() 
			throws NotOwningTransactionException, TransactionStorageInfrastructureException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException, InterruptedException, IllegalEndPositionException{
		tracker.clear(seriesId);

		String lastId = "my custom Id";
		// finish
		SequentialTransaction transaction = createPerProcessorCoordinator().startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNull(transaction);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofSeconds(120*timeScale)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());

		transaction.setTransactionId(lastId);
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, null, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertEquals(lastId, transaction.getTransactionId());
		
		try{
			createPerProcessorCoordinator().finishTransaction(seriesId, processorId, "another transaction");
			fail("should throw NoSuchTransactionException");
		}catch(NoSuchTransactionException e){}
		
		try{
			createPerProcessorCoordinator().finishTransaction(seriesId, "another processor", lastId);
			fail("should throw NotOwningTransactionException");
		}catch(NotOwningTransactionException e){}
		
		assertTrue(tracker.isTransactionSuccessful("another progress", lastId));
		assertTrue(tracker.isTransactionSuccessful(seriesId, "another transaction"));
		assertFalse(tracker.isTransactionSuccessful(seriesId, lastId));
		
		tracker.finishTransaction(seriesId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(seriesId, lastId));
		
		// abort
		transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNull(transaction);
		transaction = tracker.startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofSeconds(120*timeScale)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());

		transaction.setTransactionId(null);
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(seriesId, lastId, "010", transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		try{
			tracker.abortTransaction(seriesId, processorId, "another transaction");
			fail("should throw NoSuchTransactionException");
		}catch(NoSuchTransactionException e){}
		
		try{
			tracker.abortTransaction(seriesId, "another processor", lastId);
			fail("should throw NotOwningTransactionException");
		}catch(NotOwningTransactionException e){}
		
		assertFalse(createPerProcessorCoordinator().isTransactionSuccessful(seriesId, lastId));
		
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(seriesId);
		assertNotNull(transactions);
		assertEquals(2, transactions.size());
		assertEquals(lastId, transactions.get(1).getTransactionId());
		assertTrue(transactions.get(1).isInProgress());
		assertEquals("010", SequentialTransactionsCoordinator.getFinishedPosition(transactions));
		assertEquals("020", SequentialTransactionsCoordinator.getLastPosition(transactions));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFinished()));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isInProgress()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFailed()));
		
		tracker.abortTransaction(seriesId, processorId, lastId);
		transactions = tracker.getRecentTransactions(seriesId);
		assertNotNull(transactions);
		assertEquals(2, transactions.size());
		assertEquals(lastId, transactions.get(1).getTransactionId());
		assertTrue(transactions.get(1).isFailed());
		
		//timeout retry
		transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofMillis(1000), 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals(2, transaction.getAttempts());

		assertFalse(createPerProcessorCoordinator().isTransactionSuccessful(seriesId, lastId));
		Thread.sleep(2000);
		assertFalse(createPerProcessorCoordinator().isTransactionSuccessful(seriesId, lastId));
		
		// finish retry
		transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(2*timeScale), 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals(3, transaction.getAttempts());

		assertFalse(tracker.isTransactionSuccessful(seriesId, lastId));
		tracker.renewTransactionTimeout(seriesId, processorId, lastId, Duration.ofSeconds(120));
		Thread.sleep(2000L);
		
		tracker.updateTransactionEndPosition(seriesId, processorId, lastId, "999");
		tracker.updateTransaction(seriesId, processorId, lastId, null, Duration.ofSeconds(120), "The new detail");
		transactions = tracker.getRecentTransactions(seriesId);
		assertTrue(transactions.size() > 0);
		assertEquals("999", transactions.get(transactions.size() - 1).getEndPosition());
		assertEquals("The new detail", transactions.get(transactions.size() - 1).getDetail());
		
		
		tracker.finishTransaction(seriesId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(seriesId, lastId));
	}
	
	@Test
	public void test12OpenRangeTransactions() 
			throws NotOwningTransactionException, TransactionStorageInfrastructureException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException, InterruptedException, IllegalEndPositionException{
		tracker.clear(seriesId);

		String lastId = "my custom Id";
		// finish normal
		SequentialTransaction transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNull(transaction);
		transaction = tracker.startTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());

		transaction.setTransactionId(lastId);
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, null, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertEquals(lastId, transaction.getTransactionId());
		
		tracker.finishTransaction(seriesId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(seriesId, lastId));
		
		// open range finish
		transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNull(transaction);
		transaction = tracker.startTransaction(seriesId, processorId, Duration.ofSeconds(120*timeScale), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());

		transaction.setTransactionId(null);
		transaction.setStartPosition("011");
		transaction.setEndPositionNull();
		transaction.setDetail(transactionDetail);
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, lastId, "010", transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		assertFalse(createPerProcessorCoordinator().isTransactionSuccessful(seriesId, lastId));
		
		tracker.finishTransaction(seriesId, processorId, lastId, "015");
		
		transaction = tracker.startAnyFailedTransaction(seriesId, processorId, Duration.ofMillis(100*timeScale), 5, 5);
		assertNull(transaction);
		transaction = tracker.startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofMillis(100*timeScale)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("015", transaction.getStartPosition());
		
		// open range abort
		transaction.setTransactionId(null);
		transaction.setEndPositionNull();
		transaction.setDetail(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(10*timeScale));
		transaction = createPerProcessorCoordinator().startTransaction(seriesId, lastId, "015", transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		assertFalse(tracker.isTransactionSuccessful(seriesId, lastId));
		tracker.abortTransaction(seriesId, processorId, lastId);
		
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(seriesId);
		assertNotNull(transactions);
		assertEquals(1, transactions.size());
		assertTrue(transactions.get(0).isFinished());
		assertEquals("015", SequentialTransactionsCoordinator.getFinishedPosition(transactions));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFinished()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isInProgress()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFailed()));

	}
	
	@Test
	public void test12AbortFirstOpenRange() throws TransactionStorageInfrastructureException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException{
		tracker.clear(seriesId);
		
		// start a new one
		SequentialTransaction transaction = tracker.startTransaction(seriesId, processorId, Duration.ofMinutes(2), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		String previousId = transaction.getTransactionId();
		assertNull(previousId);
		
		transaction.setTransactionId(null);
		transaction.setStartPosition("1");
		transaction.setEndPositionNull();
		transaction = tracker.startTransaction(seriesId, null, null, transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		tracker.abortTransaction(seriesId, processorId, transaction.getTransactionId());
		
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(seriesId);
		assertEquals(0, transactions.size());
		
		transaction.setTransactionId(null);
		transaction.setStartPosition("1");
		transaction.setEndPositionNull();
		transaction = tracker.startTransaction(seriesId, null, null, transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		
		transactions = tracker.getRecentTransactions(seriesId);
		assertEquals(1, transactions.size());
		
		tracker.abortTransaction(seriesId, processorId, transaction.getTransactionId());
		transactions = tracker.getRecentTransactions(seriesId);
		assertEquals(0, transactions.size());
	}
	
	@Test(expected = IllegalEndPositionException.class)
	public void test13FinishWithNewEndAfterNextCreated() throws TransactionStorageInfrastructureException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException{
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
		//String secondId = transaction.getTransactionId();
		
		// finish the first one with another end position
		tracker.finishTransaction(seriesId, processorId, firstId, "3");
		fail("non-last transaction should not be able to finish with a new end position");
	}
	
	@Test
	public void test13CloseOpenRangeThenFinishAfterNextCreated() throws TransactionStorageInfrastructureException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException{
		tracker.clear(seriesId);
		
		// start a new open range one
		SequentialTransaction transaction = tracker.startTransaction(seriesId, processorId, Duration.ofMinutes(2), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		String previousId = transaction.getTransactionId();
		transaction.setTransactionId(null);
		transaction.setStartPosition("1");
		transaction.setEndPositionNull();
		transaction = tracker.startTransaction(seriesId, previousId, null, transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		String firstId = transaction.getTransactionId();
		
		// set end position
		tracker.updateTransactionEndPosition(seriesId, processorId, firstId, "5");
		
		// start another one:
		transaction = tracker.startTransaction(seriesId, null, null, new SimpleSequentialTransaction(processorId, Duration.ofMinutes(2)), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(firstId, transaction.getTransactionId());
		assertEquals("5", transaction.getStartPosition());
		
		transaction.setTransactionId(null);
		transaction.setStartPosition("6");
		transaction.setEndPosition("6");
		transaction = tracker.startTransaction(seriesId, firstId, "5", transaction, 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		String secondId = transaction.getTransactionId();
		
		// finish the second one
		tracker.finishTransaction(seriesId, processorId, secondId);
		
		// finish the first one
		tracker.finishTransaction(seriesId, processorId, firstId);
	}

	
	@Test
	public void test13CreateAfterLastFinishedWithNewEnd() throws TransactionStorageInfrastructureException, DuplicatedTransactionIdException, NotOwningTransactionException, IllegalTransactionStateException, NoSuchTransactionException, IllegalEndPositionException{
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



	@Test
	public void test20RandomCases() throws Exception{
		tracker.clear(seriesId);

		AtomicBoolean runFlag = new AtomicBoolean(false);
		Map<Integer, AtomicInteger> logMap = new PutIfAbsentMap<>(new HashMap<Integer, AtomicInteger>(), AtomicInteger.class);
		BasicFrequencyCounter attemptsFrequencyCounter = new BasicFrequencyCounter();
		AtomicInteger newTransactionCount = new AtomicInteger(0);
		AtomicInteger retryTransactionCount = new AtomicInteger(0);
		
		ExecutorService threads = Executors.newFixedThreadPool(NUM_PROCESSORS);
		for (int i = 0; i < NUM_PROCESSORS; i ++){
			RandomProcessor processor = new RandomProcessor(runFlag, "processor" + i, logMap, attemptsFrequencyCounter,
					newTransactionCount, retryTransactionCount);
			threads.execute(processor);
		}
		
		runFlag.set(true);
		for (;;){
			Thread.sleep(1000);
			try{
				StringBuilder sb = new StringBuilder();
				sb.append(":   ");
				List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(seriesId);
				String endPosition = null;
				for (ReadOnlySequentialTransaction transaction: transactions){
					if (transaction.isFinished()){
						endPosition = transaction.getEndPosition();
					}else{
						break;
					}
				}
				for (ReadOnlySequentialTransaction transaction: transactions){
					sb.append(transaction.getState().name().substring(0, 2)).append(" ");
				}
				sb.append(" =>").append(endPosition);
				logger.debug(sb.toString());
				
				if(transactions.size() == 1){
					ReadOnlySequentialTransaction tx = transactions.get(0);
					if (tx.isFinished() && tx.getDetail() != null && tx.getDetail().equals(Integer.valueOf(END_POSITION))){
						logger.info("All finished.");
						break;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		runFlag.set(false);
		
		assertEquals(String.valueOf(END_POSITION), SequentialTransactionsCoordinator.getFinishedPosition(tracker.getRecentTransactions(seriesId)));
		List<Integer> processed = logMap.keySet().stream().sorted().collect(Collectors.toList());
		assertEquals("Each should have been processed", END_POSITION - START_POSITION + 1, processed.size());
		for(int i = START_POSITION, j=0; i <= END_POSITION; i ++, j++){
			assertEquals(i, processed.get(j).intValue());
		}
		assertTrue("Each should have been processed once and only once: " + logMap, logMap.values().stream().allMatch(count -> count.get() == 1));
		System.out.println("\nTransactions started (new/retry/total): " 
				+ newTransactionCount.get() + "/" + retryTransactionCount.get() + "/" + (newTransactionCount.get() + retryTransactionCount.get()));
		System.out.println("Distribution of attempts:");
		System.out.println(attemptsFrequencyCounter);
		
		threads.shutdown();
	}
	
	class RandomProcessor implements Runnable{
		private AtomicBoolean runFlag;
		private String processorId;
		private Map<Integer, AtomicInteger> logMap;
		private BasicFrequencyCounter attemptsFrequencyCounter;
		private AtomicInteger newTransactionCount;
		private AtomicInteger retryTransactionCount;
		
		private Random random = new Random(System.currentTimeMillis());
		private Duration timeoutDuration = BASE_TIMEOUT_DURATION.multipliedBy(timeScale);
		
		private SequentialTransactionsCoordinator tracker;
		
		RandomProcessor(AtomicBoolean runFlag, String processorId, Map<Integer, AtomicInteger> logMap,
				BasicFrequencyCounter attemptsFrequencyCounter, AtomicInteger newTransactionCount, AtomicInteger retryTransactionCount){
			this.runFlag = runFlag;
			this.processorId = processorId;
			this.logMap = logMap;
			this.attemptsFrequencyCounter = attemptsFrequencyCounter;
			this.newTransactionCount = newTransactionCount;
			this.retryTransactionCount = retryTransactionCount;
			
			this.tracker = createPerProcessorCoordinator();
		}

		@Override
		public void run() {
			while(!runFlag.get()){};
			while(runFlag.get()){
				long startTime = System.currentTimeMillis();
				int attempts = 0;
				SequentialTransaction transaction = null;
				try{
					if (processorId.equals("processor0")){
						runFlag.get();
					}
					attempts++;
					transaction = tracker.startAnyFailedTransaction(seriesId, processorId, timeoutDuration, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
					if (processorId.equals("processor0")){
						runFlag.get();
					}
					while (transaction == null){
						attempts++;
						transaction = tracker.startTransaction(seriesId, processorId, timeoutDuration, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
					}
					if (processorId.equals("processor0")){
						runFlag.get();
					}
					while (transaction != null && !transaction.hasStarted()){
						String previousId = transaction.getTransactionId();
						String previousPosition = transaction.getStartPosition();
						transaction.setTransactionId(null);
						int position = START_POSITION;
						if (previousPosition != null){
							position = Integer.parseInt(previousPosition) + 1;
						}
						if (position > END_POSITION){
							transaction = null;
							break;
						}
						transaction.setStartPosition(String.valueOf(position));
						if (position < END_POSITION*0.6 || position > END_POSITION*0.7){
							transaction.setEndPosition(String.valueOf(position));
						}else{
							transaction.setEndPositionNull();	// for an open range transaction
						}
						transaction.setDetail(position);
						transaction.setTimeout(timeoutDuration);
						if (processorId.equals("processor0")){
							runFlag.get();
						}
						attempts++;
						transaction = tracker.startTransaction(seriesId, previousId, previousPosition, transaction, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
						if (processorId.equals("processor0")){
							runFlag.get();
						}
					}
				}catch(TransactionStorageInfrastructureException e){
					//Throwable x = e.getCause();
					// ignore
				}catch(DuplicatedTransactionIdException e){
					e.printStackTrace();
				}catch(Exception e){
					e.printStackTrace();
				}
				if (transaction != null && transaction.hasStarted()){
					logger.debug("Got a {} transaction {} [{}-{}] after {} attempts: {}", 
							(transaction.getAttempts() == 1 ? "new" : "failed"),
							transaction.getTransactionId(),
							transaction.getStartPosition(), transaction.getEndPosition(),
							attempts,
							DurationFormatter.formatSince(startTime));
					doTransaction(transaction);
				}else{
					Uninterruptibles.sleepUninterruptibly(500 * timeScale, TimeUnit.MILLISECONDS);
				}
			}
			
		}
		
		protected void doTransaction(SequentialTransaction transaction){
			if (transaction.getAttempts() > 1){
				retryTransactionCount.incrementAndGet();
			}else{
				newTransactionCount.incrementAndGet();
			}
			
			long snapTime = random.nextInt((int)(timeoutDuration.toMillis() * 1.4));
			if (snapTime > timeoutDuration.toMillis()*0.8 && transaction.getAttempts() < 10){
				Uninterruptibles.sleepUninterruptibly(snapTime, TimeUnit.MILLISECONDS);
			}

			int dice = random.nextInt(100);
			if (dice < 70 || transaction.getAttempts() >= 5){	// finish
				if (dice < 10){
					try {
						tracker.renewTransactionTimeout(seriesId, processorId, transaction.getTransactionId(), timeoutDuration);
					} catch (NotOwningTransactionException | IllegalTransactionStateException | NoSuchTransactionException e) {
						// ignore
					} catch(TransactionStorageInfrastructureException e){
						e.printStackTrace();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
				
				try {
					int p = Integer.parseInt(transaction.getStartPosition());
					if (dice < 35){
						tracker.finishTransaction(seriesId, processorId, transaction.getTransactionId());
						logMap.get(p).incrementAndGet();
						logger.debug("Finished transaction {} [{}-{}], p={}", transaction.getTransactionId(), transaction.getStartPosition(), transaction.getEndPosition(), p);
					}else{
						if (p > END_POSITION*0.5 && p < END_POSITION*0.65){
							tracker.finishTransaction(seriesId, processorId, transaction.getTransactionId(), String.valueOf(p + 4));
							logMap.get(p).incrementAndGet();
							logMap.get(p+1).incrementAndGet();
							logMap.get(p+2).incrementAndGet();
							logMap.get(p+3).incrementAndGet();
							logMap.get(p+4).incrementAndGet();
							logger.debug("Finished transaction {} [{}-{}], p={}, p+4={}", transaction.getTransactionId(), transaction.getStartPosition(), transaction.getEndPosition(), p, p+4);
						}else{
							tracker.finishTransaction(seriesId, processorId, transaction.getTransactionId(), String.valueOf(p));
							logMap.get(p).incrementAndGet();
							logger.debug("Finished transaction {} [{}-{}], p={}", transaction.getTransactionId(), transaction.getStartPosition(), transaction.getEndPosition(), p);
						}
					}
					attemptsFrequencyCounter.count(transaction.getAttempts(), 1);
				} catch (NotOwningTransactionException | IllegalTransactionStateException | IllegalEndPositionException | NoSuchTransactionException e) {
					// ignore
				} catch(TransactionStorageInfrastructureException e){
					e.printStackTrace();
				} catch(Exception e){
					e.printStackTrace();
				}
			}else if (dice < 95){
				try {
					tracker.abortTransaction(seriesId, processorId, transaction.getTransactionId());
				} catch (NotOwningTransactionException | IllegalTransactionStateException | NoSuchTransactionException e) {
					// ignore
				} catch(TransactionStorageInfrastructureException e){
					e.printStackTrace();
				} catch(Exception e){
					e.printStackTrace();
				}
			}else{
				// do nothing for time out
			}
		}
		
	}

}
