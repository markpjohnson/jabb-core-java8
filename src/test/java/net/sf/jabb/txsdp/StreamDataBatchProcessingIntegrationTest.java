/**
 * 
 */
package net.sf.jabb.txsdp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import net.sf.jabb.azure.AzureEventHubUtility;
import net.sf.jabb.azure.EventHubAnnotations;
import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.StreamDataSupplierWithIdAndRange;
import net.sf.jabb.seqtx.azure.AzureSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.Options;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.Status;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.StreamStatus;
import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.apache.qpid.amqp_1_0.jms.impl.MessageImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * @author James Hu
 *
 */
public class StreamDataBatchProcessingIntegrationTest {
	static private final Logger logger = LoggerFactory.getLogger(StreamDataBatchProcessingIntegrationTest.class);
	
	static final int NUM_PROCESSORS = 10;
	
	static protected AzureSequentialTransactionsCoordinator createCoordinator()  throws InvalidKeyException, URISyntaxException, TransactionStorageInfrastructureException{
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		AzureSequentialTransactionsCoordinator tracker = new AzureSequentialTransactionsCoordinator(storageAccount, "TestTable");
		tracker.clearAll();
		return tracker;
	}


	@Test
	public void test() throws Exception {
		List<StreamDataSupplierWithId<String>> suppliersWithId = AzureEventHubUtility.createStreamDataSuppliers(
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"),
				AzureEventHubUtility.DEFAULT_CONSUMER_GROUP,
				msg -> {
					String json = "";
					try {
						json = msg.getStringProperty(MessageImpl.JMS_AMQP_MESSAGE_ANNOTATIONS);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return json;
				});
		assertTrue(suppliersWithId.size() >= 4);
		
		List<StreamDataSupplierWithIdAndRange<String>> suppliersWithIdAndRange = suppliersWithId.stream()
				.map(s->{
					try {
						s.getSupplier().start();
						StreamDataSupplierWithIdAndRange<String> result = AttemptStrategy.create().withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
								.retryIfException()
								.retryIfResult((StreamDataSupplierWithIdAndRange<String> r) -> r == null || r.getFromPosition() == null)
								.call(()->s.withRange(Instant.now().minus(Duration.ofMinutes(30)), null, Duration.ofSeconds(30)));
						return result;
					} catch (Exception e) {
						return null;
					}
				})
				.filter(s -> s != null)
				.collect(Collectors.toList());
		assertEquals(suppliersWithId.size(), suppliersWithIdAndRange.size());
		
		for (StreamDataSupplierWithIdAndRange<String> supplierWithIdAndRange: suppliersWithIdAndRange){
			logger.info("Range to be processed in {}: ({} - {}]", supplierWithIdAndRange.getId(), supplierWithIdAndRange.getFromPosition(), supplierWithIdAndRange.getToPosition());
			assertNotNull(supplierWithIdAndRange.getFromPosition());
			assertNull(supplierWithIdAndRange.getToPosition());
		}

		
		Options options = new Options()
			.withInitialTransactionTimeoutDuration(Duration.ofMinutes(1))
			.withMaxInProgressTransactions(10)
			.withMaxRetringTransactions(10)
			.withTransactionAcquisitionDelay(Duration.ofSeconds(30))
			.withWaitStrategy(WaitStrategies.threadSleepStrategy());
		
		TransactionalStreamDataBatchProcessing<String> job = new TransactionalStreamDataBatchProcessing<>("TestJob", options, createCoordinator(), 
			(context, data) -> {
				if (data.size() > 0){
					String first = data.get(0);
					String last = data.get(data.size() - 1);
					logger.info("[{} {}] Processing {} items: {} - {}", context.getTransactionSeriesId(), context.getProcessorId(),
							data.size(), new EventHubAnnotations(first), new EventHubAnnotations(last));
					try {
						long sleepTime = 100*data.size();
						if (sleepTime > 2*1000L){
							sleepTime = 2*1000L;
						}
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				return true;
		}, 3000, Duration.ofSeconds(60), Duration.ofSeconds(10),
		suppliersWithIdAndRange);
		
		ExecutorService threadPool = Executors.newCachedThreadPool();
		for (int i = 0; i < NUM_PROCESSORS; i ++){
			Runnable runnable = job.createProcessor(String.valueOf(i));
			threadPool.execute(runnable);
		}
		
		logger.info("Starting {} processors in their threads", NUM_PROCESSORS);
		job.startAll();
		
		for (int i = Integer.MAX_VALUE; i >= 0; i --){
			Thread.sleep(Duration.ofMinutes(1).toMillis());
			try{
				Status status = job.getStatus();
				logger.info("Status: {}", status);
				
				Instant recent = Instant.now().minus(Duration.ofMinutes(2));
				if (status.getStreamStatus().values().stream()
					.filter(s -> s.getFinishedEnqueuedTime() == null || s.getFinishedEnqueuedTime().isBefore(recent))
					.count() == 0){
					logger.info("Caught up with the stream");
					if (i > 1){
						i = 1;
					}
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
		logger.info("Stopping {} processors", NUM_PROCESSORS);
		job.stopAll();
		Thread.sleep(30*1000L);
		
		for (StreamDataSupplierWithIdAndRange<String> supplierWithIdAndRange: suppliersWithIdAndRange){
			supplierWithIdAndRange.getSupplier().stop();
		}
		
		threadPool.shutdown();
	}

}
