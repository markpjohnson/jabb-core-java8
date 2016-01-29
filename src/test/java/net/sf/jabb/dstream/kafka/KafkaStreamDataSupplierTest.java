package net.sf.jabb.dstream.kafka;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

// This test class is not finished. This need following command lines setup before run it.
// 
// bin/zookeeper-server-start.sh config/zookeeper.properties
// bin/kafka-server-start.sh config/server.properties &
// bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic testTopic --partitions 1
//
public class KafkaStreamDataSupplierTest {
	private static KafkaConsumer<Void, String> realConsumer;
	private static final List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
	private static final String testTopicName = "testTopic";
	private static final int testPartition = 0;
	private static Process zooShell;
	private static Process kafkaServer;
	private long firstOffset = 0;
	private long lastOffset = 9;
	private String firstOffsetStr = "0";
	private String lastOffsetStr = "9";
	private static final int messageCount = 10;
	private final String firstMsg = "string1";
	private static final String[][] zookeeperProperties = { { "dataDir", "/tmp/zookeeper" }, { "clientPort", "2181" },
			{ "maxClientCnxns", "0" }, };

	private static final String[][] kafkaBrokerProperties = { { "broker.id", "0" },
			{ "listeners", "PLAINTEXT://localhost:9092" },
			{ "num.network.threads", "3" }, { "num.io.threads", "8" }, { "socket.send.buffer.bytes", "102400" },
			{ "socket.receive.buffer.bytes", "102400" }, { "socket.request.max.bytes", "104857600" },
			{ "log.dirs", "/tmp/kafka-logs" }, { "num.partitions", "1" }, { "num.recovery.threads.per.data.dir", "1" },
			{ "log.retention.minutes", "10" }, // at most 10 minutes test run
			{ "log.segment.bytes", "1073741824" }, { "log.retention.check.interval.ms", "10000" },
			{ "offsets.topic.num.partitions", "1" },
			{ "zookeeper.connect", "localhost:2181" },
			{ "zookeeper.connection.timeout.ms", "6000" } };

	private static String createProperties(String filename, String[][] pairs) {
		String tmpDir = System.getProperty("java.io.tmpdir");
		String fullname = tmpDir + "/" + filename;
		try {
			FileOutputStream fout = new FileOutputStream(new File(fullname));
			Properties properties = new Properties();
			for (String[] pair : pairs) {
				properties.setProperty(pair[0], pair[1]);
			}
			properties.store(fout, fullname);
		} catch (FileNotFoundException e) {
			System.out.println("Failed to open file " + fullname + ":" + e);
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return fullname;
	}

	private static Process startProcess(String directory, String... cmd) {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.directory(new File(directory));
		try {
			return pb.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Failed to start zookeeper, err=" + e);
			System.exit(-1);
		}
		return null;
	}

	@BeforeClass
	public static void setUpClass() throws InterruptedException {
		String kafkaHome = System.getenv("KAFKA_HOME");
		assertNotNull("KAFKA_HOME environment variable is not set", kafkaHome);
		String zooCfg = createProperties("zookeeper.properties", zookeeperProperties);
		String kafkaCfg = createProperties("kafkabroker.properties", kafkaBrokerProperties);
		{
			zooShell = startProcess(kafkaHome, "bin/zookeeper-server-start.sh", zooCfg);
			kafkaServer = startProcess(kafkaHome, "bin/kafka-server-start.sh", kafkaCfg);
			Process kafkaTopic = startProcess(kafkaHome, "bin/kafka-topics.sh", "--zookeeper localhost:2181",
					"--create",
					"--topic testTopic", "--partitions 1");
			int ret = kafkaTopic.waitFor();
			if (ret != 0) {
				System.out.printf("kafka create topic return ", ret);
			}
		}

		System.out.println("kafka runs correctly");
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
		// Thread.sleep(5000);
		topicPartitions.add(new TopicPartition(testTopicName, testPartition));
		Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", "localhost:9092");
		consumerProps.put("group.id", "test");
		consumerProps.put("enable.auto.commit", "false");
		// consumerProps.put("auto.commit.interval.ms", "1000");
		consumerProps.put("session.timeout.ms", "30000");
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		realConsumer = new KafkaConsumer<Void, String>(consumerProps);
		kafkaStream = new KafkaStreamDataSupplier<String>(realConsumer, topicPartitions);
	}

	@AfterClass
	public static void tearDownClass() {
		System.out.println("afterclass teardown called");
		String kafkaHome = System.getenv("KAFKA_HOME");
		assertNotNull("KAFKA_HOME environment variable is not set", kafkaHome);

		kafkaServer.destroy();
		zooShell.destroy();

		try {
			Process rm = startProcess("/tmp", "/bin/rm", "-rf", "/tmp/zookeeper", "/tmp/kafka-logs");
			int ret = rm.waitFor();
			System.out.printf("rm leftover return %d\n", ret);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("tearDown finished");
	}

	private static KafkaStreamDataSupplier<String> kafkaStream;

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
		ReceiveStatus ret = kafkaStream.fetch(out, firstOffsetStr, lastOffsetStr, 1, Duration.ofMillis(1000));
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
			return new Long(1000);
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
