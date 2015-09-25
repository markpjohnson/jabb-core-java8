/**
 * 
 */
package net.sf.jabb.dstream.mock;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.seqtx.mem.InMemSequentialTransactionsCoordinator;
import net.sf.jabb.txsdp.ProcessingContext;
import net.sf.jabb.txsdp.SimpleBatchProcessor;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.Options;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.State;
import net.sf.jabb.txsdp.TransactionalStreamDataBatchProcessing.Status;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * @author James Hu
 *
 */
public class BatchProcessingMockedStreamDataTest {
	static private ObjectMapper objectMapper = new ObjectMapper();
	
	@Test
	public void test() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException, InterruptedException{
		doTest(-10, 5, 1);
		doTest(-10, 5, 100);
		doTest(-10, 11, 10);
	}
	
	protected void doTest(int startFromMinutes, int totalMinutes, int eventsPerSecond) throws TransactionStorageInfrastructureException, DataStreamInfrastructureException, InterruptedException{
		Instant streamStartTime = Instant.now().plus(Duration.ofMinutes(startFromMinutes));
		Instant streamEndTime = streamStartTime.plus(Duration.ofMinutes(totalMinutes));
		System.out.println("Range: (" + streamStartTime.toEpochMilli() + "-" + streamEndTime.toEpochMilli() + "]");
		StreamDataSupplier<String> sds = new MockedStreamDataSupplier(eventsPerSecond, streamStartTime, streamEndTime);

		Options options = new Options()
			.withInitialTransactionTimeoutDuration(Duration.ofSeconds(10))
			.withMaxInProgressTransactions(10)
			.withMaxRetringTransactions(10)
			.withTransactionAcquisitionDelay(Duration.ofSeconds(2))
			.withWaitStrategy(WaitStrategies.threadSleepStrategy());
		
		AtomicLong totalEvents = new AtomicLong(0);
		Set<Integer> all1m = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

		TransactionalStreamDataBatchProcessing<String> processing = new TransactionalStreamDataBatchProcessing<String>("Test", options, 
				new InMemSequentialTransactionsCoordinator(),
				(context, data) ->{
					if (data.size() > 0){
						for (String jsonString: data){
							try {
								JsonNode node = objectMapper.readTree(jsonString);
								int s1 = node.get("s1").asInt();
								int s5 = node.get("s5").asInt();
								int s10 = node.get("s10").asInt();
								int s30 = node.get("s30").asInt();
								int m1 = node.get("m1").asInt();
								int m5 = node.get("m5").asInt();
								int m10 = node.get("m10").asInt();
								int m30 = node.get("m30").asInt();
								String timestampString = node.get("timestampString").asText();
								
								totalEvents.incrementAndGet();
								all1m.add(m1);
								
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						try {
							System.out.println("[" + objectMapper.readTree(data.get(0)).get("timestamp").asText() + "-"
									+ objectMapper.readTree(data.get(data.size() - 1)).get("timestamp").asText() + "]");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					return true;
				},
				300, Duration.ofSeconds(5), Duration.ofSeconds(2),
				ImmutableList.of(sds.withId("Test Stream").withRange(streamStartTime, streamEndTime))
		);
		
		new Thread(processing.createProcessor("processor1")).start();
		new Thread(processing.createProcessor("processor2")).start();
		new Thread(processing.createProcessor("processor3")).start();
		new Thread(processing.createProcessor("processor4")).start();
		processing.startAll();
		
		Status status;
		while(true){
			status = processing.getStatus();
			System.out.println(status);
			if (status.getProcessorStatus().values().stream().allMatch(s->s.getState() == State.FINISHED)){
				break;
			}
			Thread.sleep(2000);
		}
		processing.stopAll();
		
		
		assertEquals(60L * totalMinutes * eventsPerSecond, totalEvents.get());
		if (totalMinutes < 60){
			assertTrue(totalMinutes == all1m.size() || totalMinutes + 1 == all1m.size());
		}
	}

}
