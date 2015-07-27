/**
 * 
 */
package net.sf.jabb.seqtx;

import static org.junit.Assert.*;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionState;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.IllegalEndPositionException;
import net.sf.jabb.seqtx.ex.IllegalTransactionStateException;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.ex.NoSuchTransactionException;
import net.sf.jabb.seqtx.ex.NotOwningTransactionException;
import net.sf.jabb.util.col.PutIfAbsentMap;
import net.sf.jabb.util.stat.BasicFrequencyCounter;

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
public abstract class SequentialTransactionsCoordinatorTest {
	protected final int NUM_PROCESSORS = 20;
	protected final int MAX_IN_PROGRESS_TRANSACTIONS = 10;
	protected final int MAX_RETRYING_TRANSACTIONS = 5;
	protected final int START_POSITION = 1;
	protected final int END_POSITION = 500;
	protected final Duration TIMEOUT_DURATION = Duration.ofMillis(100);
	
	protected SequentialTransactionsCoordinator tracker;
	protected String progressId = "Test progress Id 1";
	protected String processorId = "Test processor 1";
	protected String transactionDetail = "This is the transaction detail";
	
	protected SequentialTransactionsCoordinatorTest(){
		tracker = createTracker();
	}

	abstract protected SequentialTransactionsCoordinator createTracker();
	
	@Test
	public void test09ClearTransactions() throws InfrastructureErrorException{
		tracker.clear(progressId);
	}
	
	@Test
	public void test10StartTransactions() throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException{
		tracker.clear(progressId);
		
		String lastId;
		// empty
		SequentialTransaction transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertNull(transaction.getTransactionId());
		assertNull(transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		
		transaction = tracker.startTransaction(progressId, null, transaction, 5, 5); // in-progress
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
		assertEquals(1, tracker.getRecentTransactions(progressId).size());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(0).getState());
	
		lastId = transaction.getTransactionId();
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);	// in-progress
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120));
		
		transaction = tracker.startTransaction(progressId, "alksdjflksdj", transaction, 5, 5);  // will get a skeleton
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("010", transaction.getStartPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());

		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction.setTimeout(Duration.ofSeconds(120));

		try{
			tracker.startTransaction(progressId, lastId, transaction, 5, 5);
			fail("should throw DuplicatedTransactionIdException");
		}catch(DuplicatedTransactionIdException e){
			// ignore
		}
		transaction.setTransactionId(null);
		transaction = tracker.startTransaction(progressId, lastId, transaction, 5, 5); // in-progress, in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertNotNull(transaction.getTransactionId());
		assertEquals("011", transaction.getStartPosition());
		assertEquals("020", transaction.getEndPosition());
		assertNotNull(transaction.getTimeout());
		assertEquals(processorId, transaction.getProcessorId());
		assertEquals(1, transaction.getAttempts());
		assertEquals(2, tracker.getRecentTransactions(progressId).size());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(0).getState());
		assertEquals(SequentialTransactionState.IN_PROGRESS, tracker.getRecentTransactions(progressId).get(1).getState());
		
		lastId = transaction.getTransactionId();
		tracker.abortTransaction(progressId, processorId, lastId);	// in-progress, aborted
		
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);	// in-progress, in-progress(retry)
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
	
	@Test
	public void test11InProgressTransactions() 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException, InterruptedException, IllegalEndPositionException{
		tracker.clear(progressId);

		String lastId = "my custom Id";
		// finish
		SequentialTransaction transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());

		transaction.setTransactionId(lastId);
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(progressId, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertEquals(lastId, transaction.getTransactionId());
		
		try{
			tracker.finishTransaction(progressId, processorId, "another transaction");
			fail("should throw NoSuchTransactionException");
		}catch(NoSuchTransactionException e){}
		
		try{
			tracker.finishTransaction(progressId, "another processor", lastId);
			fail("should throw NotOwningTransactionException");
		}catch(NotOwningTransactionException e){}
		
		assertTrue(tracker.isTransactionSuccessful("another progress", lastId));
		assertTrue(tracker.isTransactionSuccessful(progressId, "another transaction"));
		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		assertTrue(tracker.isTransactionSuccessful(progressId, "another transaction", Instant.now().plusSeconds(-3600)));
		
		tracker.finishTransaction(progressId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(progressId, lastId));
		assertTrue(tracker.isTransactionSuccessful(progressId, "another transaction", Instant.now().plusSeconds(-3600)));
		
		// abort
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());

		transaction.setTransactionId(null);
		transaction.setStartPosition("011");
		transaction.setEndPosition("020");
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(progressId, lastId, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		try{
			tracker.abortTransaction(progressId, processorId, "another transaction");
			fail("should throw NoSuchTransactionException");
		}catch(NoSuchTransactionException e){}
		
		try{
			tracker.abortTransaction(progressId, "another processor", lastId);
			fail("should throw NotOwningTransactionException");
		}catch(NotOwningTransactionException e){}
		
		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(progressId);
		assertNotNull(transactions);
		assertEquals(2, transactions.size());
		assertEquals(lastId, transactions.get(1).getTransactionId());
		assertTrue(transactions.get(1).isInProgress());
		assertEquals("010", SequentialTransactionsCoordinator.getLastFinishedPosition(transactions));
		assertEquals("020", SequentialTransactionsCoordinator.getLastPosition(transactions));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFinished()));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isInProgress()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFailed()));
		
		tracker.abortTransaction(progressId, processorId, lastId);
		transactions = tracker.getRecentTransactions(progressId);
		assertNotNull(transactions);
		assertEquals(2, transactions.size());
		assertEquals(lastId, transactions.get(1).getTransactionId());
		assertTrue(transactions.get(1).isFailed());
		
		//timeout retry
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofMillis(100), 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals(2, transaction.getAttempts());

		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		Thread.sleep(500L);
		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		
		// finish retry
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(1), 5, 5);
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals(3, transaction.getAttempts());

		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		tracker.renewTransactionTimeout(progressId, processorId, lastId, Duration.ofSeconds(120));
		Thread.sleep(2000L);
		tracker.finishTransaction(progressId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(progressId, lastId));
	}
	
	@Test
	public void test12OpenRangeTransactions() 
			throws NotOwningTransactionException, InfrastructureErrorException, IllegalTransactionStateException, NoSuchTransactionException, DuplicatedTransactionIdException, InterruptedException, IllegalEndPositionException{
		tracker.clear(progressId);

		String lastId = "my custom Id";
		// finish normal
		SequentialTransaction transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());

		transaction.setTransactionId(lastId);
		transaction.setStartPosition("001");
		transaction.setEndPosition("010");
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(progressId, null, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		assertEquals(lastId, transaction.getTransactionId());
		
		tracker.finishTransaction(progressId, processorId, lastId);
		assertTrue(tracker.isTransactionSuccessful(progressId, lastId));
		assertTrue(tracker.isTransactionSuccessful(progressId, "another transaction", Instant.now().plusSeconds(-3600)));
		
		// open range finish
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofSeconds(120), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());

		transaction.setTransactionId(null);
		transaction.setStartPosition("011");
		transaction.setEndPosition(null);
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(progressId, lastId, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		
		tracker.finishTransaction(progressId, processorId, lastId, "015");
		
		transaction = tracker.startAnyFailedTransaction(progressId, processorId, Duration.ofMillis(100), 5, 5);
		assertNotNull(transaction);
		assertFalse(transaction.hasStarted());
		assertEquals(lastId, transaction.getTransactionId());
		assertEquals("015", transaction.getStartPosition());
		
		// open range abort
		transaction.setTransactionId(null);
		transaction.setEndPosition(null);
		transaction.setDetail(transactionDetail);
		transaction = tracker.startTransaction(progressId, lastId, transaction, 5, 5); // in-progress
		assertNotNull(transaction);
		assertTrue(transaction.hasStarted());
		assertEquals(SequentialTransactionState.IN_PROGRESS, transaction.getState());
		lastId = transaction.getTransactionId();
		
		assertFalse(tracker.isTransactionSuccessful(progressId, lastId));
		tracker.abortTransaction(progressId, processorId, lastId);
		
		List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(progressId);
		assertNotNull(transactions);
		assertEquals(1, transactions.size());
		assertTrue(transactions.get(0).isFinished());
		assertEquals("015", SequentialTransactionsCoordinator.getLastFinishedPosition(transactions));
		assertEquals(1, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFinished()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isInProgress()));
		assertEquals(0, SequentialTransactionsCoordinator.getTransactionCount(transactions, tx->tx.isFailed()));

	}

	@Test
	public void test20RandomCases() throws InfrastructureErrorException, InterruptedException{
		tracker.clear(progressId);

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
		while(true){
			Thread.sleep(1000);
			List<? extends ReadOnlySequentialTransaction> transactions = tracker.getRecentTransactions(progressId);
			if (transactions.size() > 0){
				ReadOnlySequentialTransaction t0 = transactions.get(0);
				if (t0.isFinished()){
					System.out.print(t0.getEndPosition() + ".");
				}
				if(transactions.stream().anyMatch(tx -> END_POSITION ==(Integer)tx.getDetail())
					&& transactions.stream().allMatch(tx -> tx.isFinished()) ){
				break;
			}
			}
		}
		
		assertEquals(String.valueOf(END_POSITION), SequentialTransactionsCoordinator.getLastFinishedPosition(tracker.getRecentTransactions(progressId)));
		List<Integer> processed = logMap.keySet().stream().sorted().collect(Collectors.toList());
		assertEquals("Each should have been processed", END_POSITION - START_POSITION + 1, processed.size());
		for(int i = START_POSITION, j=0; i <= END_POSITION; i ++, j++){
			assertEquals(i, processed.get(j).intValue());
		}
		assertTrue("Each should have been processed once and only once", logMap.values().stream().allMatch(count -> count.get() == 1));
		System.out.println("\nTransactions started (new/retry/total): " 
				+ newTransactionCount.get() + "/" + retryTransactionCount.get() + "/" + (newTransactionCount.get() + retryTransactionCount.get()));
		System.out.println("Distribution of attempts:");
		System.out.println(attemptsFrequencyCounter);
	}
	
	class RandomProcessor implements Runnable{
		private AtomicBoolean runFlag;
		private String processorId;
		private Map<Integer, AtomicInteger> logMap;
		private BasicFrequencyCounter attemptsFrequencyCounter;
		private AtomicInteger newTransactionCount;
		private AtomicInteger retryTransactionCount;
		
		private Random random = new Random();
		
		RandomProcessor(AtomicBoolean runFlag, String processorId, Map<Integer, AtomicInteger> logMap,
				BasicFrequencyCounter attemptsFrequencyCounter, AtomicInteger newTransactionCount, AtomicInteger retryTransactionCount){
			this.runFlag = runFlag;
			this.processorId = processorId;
			this.logMap = logMap;
			this.attemptsFrequencyCounter = attemptsFrequencyCounter;
			this.newTransactionCount = newTransactionCount;
			this.retryTransactionCount = retryTransactionCount;
		}

		@Override
		public void run() {
			while(!runFlag.get()){};
			while(runFlag.get()){
				SequentialTransaction transaction = null;
				try{
					transaction = tracker.startAnyFailedTransaction(progressId, processorId, TIMEOUT_DURATION, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
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
						if (position < END_POSITION/3 || position > END_POSITION * 3/4){
							transaction.setEndPosition(String.valueOf(position));
						}else{
							transaction.setEndPosition(null);	// for an open range transaction
						}
						transaction.setDetail(position);
						transaction.setTimeout(TIMEOUT_DURATION);
						transaction = tracker.startTransaction(progressId, previousId, transaction, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
					}
				}catch(InfrastructureErrorException | DuplicatedTransactionIdException e){
					e.printStackTrace();
				}catch(Exception e){
					e.printStackTrace();
				}
				if (transaction != null){
					doTransaction(transaction);
				}else{
					Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
				}
			}
			
		}
		
		protected void doTransaction(SequentialTransaction transaction){
			if (transaction.getAttempts() > 1){
				retryTransactionCount.incrementAndGet();
			}else{
				newTransactionCount.incrementAndGet();
			}
			
			long snapTime = random.nextInt((int)TIMEOUT_DURATION.toMillis() * 2);
			Uninterruptibles.sleepUninterruptibly(snapTime, TimeUnit.MILLISECONDS);

			int dice = random.nextInt(100);
			if (dice < 70){	// finish
				if (dice < 10){
					try {
						tracker.renewTransactionTimeout(progressId, processorId, transaction.getTransactionId(), TIMEOUT_DURATION);
					} catch (NotOwningTransactionException | IllegalTransactionStateException | NoSuchTransactionException e) {
						// ignore
					} catch(InfrastructureErrorException e){
						e.printStackTrace();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
				
				try {
					int p = Integer.parseInt(transaction.getStartPosition());
					if (dice < 35){
						tracker.finishTransaction(progressId, processorId, transaction.getTransactionId());
						logMap.get(p).incrementAndGet();
					}else{
						if (p < END_POSITION - 10){
							tracker.finishTransaction(progressId, processorId, transaction.getTransactionId(), String.valueOf(p + 2));
							logMap.get(p).incrementAndGet();
							logMap.get(p+1).incrementAndGet();
							logMap.get(p+2).incrementAndGet();
						}else{
							tracker.finishTransaction(progressId, processorId, transaction.getTransactionId(), String.valueOf(p));
							logMap.get(p).incrementAndGet();
						}
					}
					attemptsFrequencyCounter.count(transaction.getAttempts(), 1);
				} catch (NotOwningTransactionException | IllegalTransactionStateException | IllegalEndPositionException | NoSuchTransactionException e) {
					// ignore
				} catch(InfrastructureErrorException e){
					e.printStackTrace();
				} catch(Exception e){
					e.printStackTrace();
				}
			}else if (dice < 90){
				try {
					tracker.abortTransaction(progressId, processorId, transaction.getTransactionId());
				} catch (NotOwningTransactionException | IllegalTransactionStateException | NoSuchTransactionException e) {
					// ignore
				} catch(InfrastructureErrorException e){
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
