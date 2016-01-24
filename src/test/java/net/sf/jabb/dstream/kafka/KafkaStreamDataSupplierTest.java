package net.sf.jabb.dstream.kafka;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

// This test class is not finished. This need following command lines setup before run it.
// 
// bin/zookeeper-server-start.sh config/zookeeper.properties
// bin/kafka-server-start.sh config/server.properties &
// bin/kafka-topics.sh --zookeeper localhost:10000 --create --topic testTopic --partitions 1
//
@Ignore
public class KafkaStreamDataSupplierTest {
	private KafkaConsumer<Void, String> realConsumer;
	private static final List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
	private static final String testTopicName = "testTopic";
	private static final int testPartition = 0;
	private long firstOffset = 0;
	private long lastOffset = 9;
	private String firstOffsetStr = String.valueOf(firstOffset);
	private String lastOffsetStr;
	private static final int messageCount = 10;
	private final String firstMsg = "string1";

	@BeforeClass
	public static void setUpClass() {
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", "localhost:9092");
		producerProps.put("acks", "all");
		producerProps.put("retries", 0);
		producerProps.put("batch.size", 16384);
		producerProps.put("linger.ms", 1);
		producerProps.put("buffer.memory", 33554432);
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<Void, String> realProducer = new KafkaProducer<>(producerProps);
		for (int i = 0; i < messageCount; i++) {
			realProducer.send(
					new ProducerRecord<Void, String>(testTopicName, testPartition, null, "string" + String.valueOf(i)));
		}
		realProducer.close();
		topicPartitions.add(new TopicPartition(testTopicName, testPartition));
	}

	@Before
	public void setUp() throws Exception {
		Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", "localhost:9092");
		consumerProps.put("group.id", "test");
		consumerProps.put("enable.auto.commit", "false");
		consumerProps.put("auto.commit.interval.ms", "1000");
		consumerProps.put("session.timeout.ms", "30000");
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		realConsumer = new KafkaConsumer<Void, String>(consumerProps);
		realConsumer.assign(topicPartitions);
		realConsumer.seekToEnd(topicPartitions.get(0));
		lastOffset = realConsumer.position(topicPartitions.get(0));
		lastOffsetStr = String.valueOf(lastOffset);
		kafkaStream = new KafkaStreamDataSupplier<String>(realConsumer, topicPartitions);
	}

	@After
	public void tearDown() {
		realConsumer.close();
	}

	private KafkaStreamDataSupplier<String> kafkaStream;

	@Test
	public void testFirstPosition() {
		if (!kafkaStream.firstPosition().equals(firstOffsetStr)) {
			fail("kafkaStream.firstPosition()=" + kafkaStream.firstPosition() + ", expect " + firstOffsetStr);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFirstPositionInstantDuration() throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.firstPosition(Instant.now());
	}

	@Test
	public void testLastPosition() throws DataStreamInfrastructureException {
		String lastPos = kafkaStream.lastPosition();
		if (!lastPos.equals(lastOffsetStr)) {
			fail("kafkaStream.LastPosition()=" + lastPos + ", expect " + lastOffsetStr);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testEnqueuedTime() throws DataStreamInfrastructureException {
		kafkaStream.enqueuedTime(firstOffsetStr);
	}

	@Test
	public void testNextStartPosition() {
		long next = lastOffset + 1;
		if (!kafkaStream.nextStartPosition(lastOffsetStr).equals(String.valueOf(next))) {
			fail("kafkaStream.nextStartPosition()=" + kafkaStream.nextStartPosition(lastOffsetStr) + ", expected "
					+ String.valueOf(next));
		}
	}

	@Test
	public void testIsInRangeStringString() {
		assertTrue(kafkaStream.isInRange("1", "2"));
		assertTrue(kafkaStream.isInRange("2", null));
		assertTrue(!kafkaStream.isInRange("2", "1"));
	}

	@Test
	public void testIsInRangeInstantInstant() {
		// IsInRange(Instant, Instant) always return false.
		assertTrue(!kafkaStream.isInRange(Instant.MIN, Instant.MAX));
		assertTrue(!kafkaStream.isInRange(Instant.MAX, Instant.MIN));
	}

	// @Ignore
	@Test
	public void testFetchListOfQsuperMStringStringIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		List<String> out = new ArrayList<String>();
		// The timeout duration needs to be longer, as this is the first time to
		// establish connection.
		ReceiveStatus ret = kafkaStream.fetch(out, firstOffsetStr, lastOffsetStr, 1, Duration.ofMillis(200));
		if (ret.isOutOfRangeReached()) {
			fail("fetch return out of range error");
		}
		if (!ret.getLastPosition().equals(firstOffsetStr)) {
			fail("fetch last position got " + ret.getLastPosition() + ", expect " + firstOffsetStr);
		}
		if (out.size() != 1) {
			fail("fetch message got " + out.size() + ", expect 1");
		}
		if (!out.get(0).equals("string" + firstOffsetStr)) {
			fail("fetch message got " + out.get(0) + ", expect string" + firstOffsetStr);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFetchListOfQsuperMInstantInstantIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.fetch(null, Instant.MIN, Instant.MAX, Duration.ofMillis(10));
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFetchListOfQsuperMStringInstantIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.fetch(null, Instant.MIN, 1, null);
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testStartAsyncReceivingConsumerOfMString() throws DataStreamInfrastructureException {
		kafkaStream.startAsyncReceiving(null, "not implemented");
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testStartAsyncReceivingConsumerOfMInstant() throws DataStreamInfrastructureException {
		kafkaStream.startAsyncReceiving(null, Instant.now());
	}

	@Test
	public void testStopAsyncReceiving() {
		kafkaStream.stopAsyncReceiving(null);
	}

	@Test
	public void testReceiveFunctionOfMLongStringString() throws DataStreamInfrastructureException {
		Function<String, Long> receiver = m -> {
			String lastMsg = m;
			if (lastMsg != null && !lastMsg.equals(firstMsg)) {
				fail("received message got " + lastMsg + ", expect " + firstMsg);
			}
			return new Long(200);
		};
		ReceiveStatus ret = kafkaStream.receive(receiver, "1", "1");
		if (!ret.getLastPosition().equals("1")) {
			fail("lastPosition got " + ret.getLastPosition() + ", expect 1");
		}
		if (!ret.isOutOfRangeReached()) {
			fail("out of range for one receive not triggered");
		}

	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testReceiveFunctionOfMLongInstantInstant() throws DataStreamInfrastructureException {
		kafkaStream.receive(null, Instant.MAX, Instant.MAX);
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testReceiveFunctionOfMLongStringInstant() throws DataStreamInfrastructureException {
		kafkaStream.receive(null, "1", Instant.MAX);
	}
}
