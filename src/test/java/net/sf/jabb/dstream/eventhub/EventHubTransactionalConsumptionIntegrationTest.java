/**
 * 
 */
package net.sf.jabb.dstream.eventhub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.WrappedJmsConnection;
import net.sf.jabb.dstream.eventhub.EventHubQpidStreamDataSupplier;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.azure.AzureSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.util.col.PutIfAbsentMap;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.Sequencer;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.stat.BasicFrequencyCounter;
import net.sf.jabb.util.stat.ConcurrentLongMinMaxHolder;
import net.sf.jabb.util.text.DurationFormatter;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;
import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * Test consuming Azure Event Hub in a transactional way by multiple processors.
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EventHubTransactionalConsumptionIntegrationTest {
	static private final Logger logger = LoggerFactory.getLogger(EventHubTransactionalConsumptionIntegrationTest.class);

	static public final int PARTITIONS = 4;
	static public final int SENDERS = 4;
	static public final int CONSUMERS = 20;
	static public final int TOTAL_MESSAGES = 5000;
	static public final int BATCH_SIZE = 100;
	static public final Duration BATCH_FETCH_DURATION = Duration.ofSeconds(1);
	
	protected final int MAX_IN_PROGRESS_TRANSACTIONS = 15;
	protected final int MAX_RETRYING_TRANSACTIONS = 10;
	
	protected final Duration TX_TIMEOUT_DURATION = Duration.ofMillis(5000);
	protected final long TX_INTERVAL = 2000;

	
	protected ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
			0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

	protected SequentialTransactionsCoordinator txCoordinator;
	protected List<StreamDataSupplier<String>> dstreams= new ArrayList<>(PARTITIONS);
	protected Queue eventHub;
	protected Queue[] partitions = new Queue[PARTITIONS];
	protected WrappedJmsConnection sendConnection = null;
	
	protected Sequencer messageSequencer = new Sequencer(1);
	protected ConcurrentLongMinMaxHolder lastSent = new ConcurrentLongMinMaxHolder(0, 0);
	protected Instant startSendingTime;
	
	protected StreamDataSupplier<String> createStreamProvider(int partition){
		return new EventHubQpidStreamDataSupplier<>(
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
				null, partition, 
				message -> {
					try{
						if (message instanceof TextMessage){
							return ((TextMessage)message).getText();
						}else if (message instanceof BytesMessage){
							BytesMessage msg = (BytesMessage) message;
							byte[] bytes = new byte[(int)msg.getBodyLength()];
							msg.readBytes(bytes, (int)msg.getBodyLength());
							return new String(bytes, StandardCharsets.UTF_8);
						}else{
							return message.getClass().getName();
						}
					}catch(Exception e){
						e.printStackTrace();
						return null;
					}
				});
	}
	
	public EventHubTransactionalConsumptionIntegrationTest(){
		eventHub = new org.apache.qpid.amqp_1_0.jms.impl.QueueImpl(System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"));
		for (int i = 0; i < PARTITIONS; i ++){
			partitions[i] = EventHubQpidStreamDataSupplier.createQueue(System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"), "$Default", i);
		}
	}

	@Test
	public void test1ValidateSetup() throws Exception {
		StreamDataSupplier<String> dstream = createStreamProvider(0);
		dstream.start();
		System.out.println("In partition 0, first position: " + dstream.firstPosition(Instant.EPOCH) + ", last position: " + dstream.lastPosition());
		dstream.stop();
	}

	@Test
	public void test2Main() throws Exception {
		Map<Integer, AtomicInteger> logMap = new PutIfAbsentMap<>(new HashMap<Integer, AtomicInteger>(), AtomicInteger.class);
		BasicFrequencyCounter attemptsFrequencyCounter = new BasicFrequencyCounter();
		AtomicInteger newTransactionCount = new AtomicInteger(0);
		AtomicInteger retryTransactionCount = new AtomicInteger(0);
		
		Thread.sleep(61*1000L);  // as buffer for the precision of startSendingTime
		startSending();
		while(lastSent.getMaxAsLong() < TOTAL_MESSAGES/8){
			System.out.print("." + lastSent.getMaxAsLong());
			Thread.sleep(500);
		}
		logger.debug("\n1/8 sent.");
		
		for (int i = 0; i < PARTITIONS; i ++){
			dstreams.add(createStreamProvider(i));
		}
		for (int i = 0; i < PARTITIONS; i ++){
			dstreams.get(i).start();
		}
		
		AtomicBoolean runFlag = startConsuming(logMap, attemptsFrequencyCounter, newTransactionCount, retryTransactionCount);
		
		
		for (int finishedPartitions = 0; finishedPartitions < (1 << PARTITIONS) - 1; ){
			Thread.sleep(2000);
			for (int i = 0; i < PARTITIONS; i ++){
				if ((finishedPartitions & (1 << i)) == 0){
					String partitionId = String.valueOf(i);
					StringBuilder sb = new StringBuilder();
					sb.append("partition " + partitionId + " :   ");
					try{
						List<? extends ReadOnlySequentialTransaction> transactions = txCoordinator.getRecentTransactions(partitionId);
						String endPosition = null;
						for (ReadOnlySequentialTransaction transaction: transactions){
							if (transaction.isFinished()){
								endPosition = transaction.getEndPosition() + "/" + transaction.getDetail();
							}else{
								break;
							}
						}
						for (ReadOnlySequentialTransaction transaction: transactions){
							sb.append(transaction.getState().name().substring(0, 2) + " ");
						}
						sb.append(" =>" + endPosition);
						logger.debug(sb.toString());
						
						if(lastSent.getMaxAsLong() >= TOTAL_MESSAGES && transactions.size() == 1){
							ReadOnlySequentialTransaction tx = transactions.get(0);
							if (tx.isFinished() && tx.getDetail() != null 
									&& (Integer)tx.getDetail() > (TOTAL_MESSAGES/2)
									&& dstreams.get(i).lastPosition().equals(tx.getEndPosition())){
								finishedPartitions |= (1 << i);
								logger.debug("All consumed in partition " + i);
							}
						}
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		}
		runFlag.set(false);
		do{
			Thread.sleep(TX_INTERVAL);
		}while(threadPool.getActiveCount() > 0);
		System.out.println("Done processing");
		for (int i = 0; i < PARTITIONS; i ++){
			dstreams.get(i).stop();
		}
		txCoordinator.clearAll();

		List<Integer> processed = logMap.keySet().stream().sorted().collect(Collectors.toList());
		assertEquals("Each should have been processed", TOTAL_MESSAGES, processed.size());
		for(int i = 1, j=0; i <= TOTAL_MESSAGES; i ++, j++){
			assertEquals(i, processed.get(j).intValue());
		}
		assertTrue("Each should have been processed once and only once: " + logMap, logMap.values().stream().allMatch(count -> count.get() == 1));
		System.out.println("\nTransactions started (new/retry/total): " 
				+ newTransactionCount.get() + "/" + retryTransactionCount.get() + "/" + (newTransactionCount.get() + retryTransactionCount.get()));
		System.out.println("Distribution of attempts:");
		System.out.println(attemptsFrequencyCounter);

	}

	private void startSending() throws InterruptedException {
		if (sendConnection == null){
			sendConnection = EventHubQpidStreamDataSupplier.createConnectionForSending(
					System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
					System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
					System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
					eventHub, 
					BackoffStrategies.fibonacciBackoff(1000, 10, TimeUnit.SECONDS), 
					WaitStrategies.threadSleepStrategy(), 
					"test");
			sendConnection.establishConnection();
		}
		
		startSendingTime = Instant.now().minus(Duration.ofSeconds(60));
		logger.debug("Start sending by " + SENDERS + " threads ...");
		for (int i = 0; i < SENDERS; i ++){
			threadPool.execute(()->{
				Session session = null;
				MessageProducer sender = null;
				try{
					while(sendConnection.getConnection() == null){
						logger.debug("Wait for send connection to be initially established");
						Thread.sleep(100);
					}
					session = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					sender = session.createProducer(eventHub);
					for (long id = messageSequencer.next(); id <= TOTAL_MESSAGES; id = messageSequencer.next()){
						TextMessage msg = session.createTextMessage();
						msg.setText(String.valueOf(id));
						sender.send(msg);
						lastSent.evaluate(id);
						if (lastSent.getMaxAsLong() > TOTAL_MESSAGES/2){
							Thread.sleep(20);
						}
					}
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					closeSilently(sender, null, session);
				}
				logger.debug("All sent.");
			});
		}
		
	}


	private AtomicBoolean startConsuming(Map<Integer, AtomicInteger> logMap, BasicFrequencyCounter attemptsFrequencyCounter, AtomicInteger newTransactionCount, AtomicInteger retryTransactionCount) throws Exception {
		AtomicBoolean runFlag = new AtomicBoolean(false);
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		this.txCoordinator = new AzureSequentialTransactionsCoordinator(storageAccount, "TestTable");
		txCoordinator.clearAll();
		String[] START_POSITIONS = new String[PARTITIONS];
		for (int i = 0; i < PARTITIONS; i ++){
			logger.debug("Finding the first/last positions in partition " + i + " after " + startSendingTime + " = " + startSendingTime.toEpochMilli());
			String pos = dstreams.get(i).firstPosition(startSendingTime, Duration.ofMinutes(15));
			String last =  dstreams.get(i).lastPosition();
			START_POSITIONS[i]  = pos;
			logger.debug("Will start consuming in partition " + i + " from " + pos + " to " + last + " or beyond");
		}

		for (int i = 0; i < CONSUMERS; i ++){
			threadPool.execute(new Runnable(){
				private String processorId;
				
				public Runnable setProcessorId(String processorId){
					this.processorId = processorId;
					return this;
				}
				
				@Override
				public void run(){
					int p = 0;
					while(runFlag.get()){
						long startTime = System.currentTimeMillis();
						int attempts = 0;
						SequentialTransaction transaction = null;
						String partitionId = null;
						int partition = p % 4;
						try{
							while (transaction == null){
								attempts++;
								partition = p++ % 4;
								partitionId = String.valueOf(partition);
								try {
									transaction = txCoordinator.startTransaction(partitionId, processorId, TX_TIMEOUT_DURATION, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
								} catch (Exception e) {
									e.printStackTrace();
									Uninterruptibles.sleepUninterruptibly(TX_INTERVAL, TimeUnit.MILLISECONDS);
								}
							}
							
							// got a skeleton, with matching seriesId
							while (transaction != null && !transaction.hasStarted()){
								String previousId = transaction.getTransactionId();
								String previousPosition = transaction.getStartPosition();
								transaction.setTransactionId(null);
								String position;
								if (previousPosition != null){
									position = dstreams.get(partition).nextStartPosition(previousPosition);
								}else{
									position = START_POSITIONS[partition];
								}
								String lastPosition = dstreams.get(partition).lastPosition();
								if (position.equals(lastPosition) || !dstreams.get(partition).isInRange(position, lastPosition)){
									transaction = null;
									break;
								}
								transaction.setStartPosition(String.valueOf(position));
								transaction.setEndPositionNull();	// for an open range transaction
								//transaction.setDetail(position);
								transaction.setTimeout(TX_TIMEOUT_DURATION);
								attempts++;
								transaction = txCoordinator.startTransaction(partitionId, previousId, previousPosition, transaction, MAX_IN_PROGRESS_TRANSACTIONS, MAX_RETRYING_TRANSACTIONS);
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
							doTransaction(partition, partitionId, processorId, transaction,
									logMap, attemptsFrequencyCounter, newTransactionCount, retryTransactionCount);
						}else{
							Uninterruptibles.sleepUninterruptibly(TX_INTERVAL, TimeUnit.MILLISECONDS);
						}
					}
				}
			}.setProcessorId("processor-" + i));
		}
		runFlag.set(true);
		return runFlag;
	}


	private void doTransaction(int partition, String seriesId, String processorId, SequentialTransaction transaction,
			Map<Integer, AtomicInteger> logMap, BasicFrequencyCounter attemptsFrequencyCounter, AtomicInteger newTransactionCount, AtomicInteger retryTransactionCount) {
		if (transaction.getAttempts() > 1){
			retryTransactionCount.incrementAndGet();
		}else{
			newTransactionCount.incrementAndGet();
		}

		String transactionId = transaction.getTransactionId();
		String startPosition = transaction.getStartPosition();
		String endPosition = transaction.getEndPosition() == null ? null : transaction.getEndPosition();
		
		List<String> messages = new ArrayList<>(BATCH_SIZE);
		try{
			endPosition = dstreams.get(partition).fetch(messages, startPosition, BATCH_SIZE, BATCH_FETCH_DURATION).getLastPosition();
			if (endPosition != null){
				logger.debug("Fetched " + messages.size() + " messages by " + processorId + " from partition " + seriesId + ": (" + startPosition + ", " + endPosition + "]");
				Integer lastMessageId = Integer.valueOf(messages.get(messages.size() - 1));
				logger.debug("Fetched messages by " + processorId + " for partition " + seriesId + ": " + messages.stream().collect(Collectors.joining(",")));
				txCoordinator.updateTransaction(seriesId, processorId, transaction.getTransactionId(), endPosition.toString(), TX_TIMEOUT_DURATION, lastMessageId);
				Thread.sleep(messages.size() * 45);
			}else{
				txCoordinator.abortTransaction(seriesId, processorId, transactionId);
			}
		}catch(Exception e){
			try {
				txCoordinator.abortTransaction(seriesId, processorId, transactionId);
			} catch (Exception e1) {
				// ignore
			}
		}
		try {
			txCoordinator.finishTransaction(seriesId, processorId, transactionId);
			attemptsFrequencyCounter.count(transaction.getAttempts(), 1);
			for (String msg: messages){
				logMap.get(Integer.valueOf(msg)).incrementAndGet();
			}

			logger.debug("Finished transaction by " + processorId + " for partition " + seriesId + ": (" + startPosition + ", " + endPosition + "]");
		} catch (Exception e) {
			// ignore
		}
	}

	protected void closeSilently(MessageProducer sender, MessageConsumer consumer, Session session){
		if (sender != null){
			try{
				sender.close();
			}catch(Exception e){
				// ignore
			}
		}
		if (consumer != null){
			try{
				consumer.close();
			}catch(Exception e){
				// ignore
			}
		}
		if (session != null){
			try{
				session.close();
			}catch(Exception e){
				// ignore
			}
		}
	}



}
