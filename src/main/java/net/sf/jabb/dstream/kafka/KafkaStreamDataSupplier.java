package net.sf.jabb.dstream.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.SimpleReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

public class KafkaStreamDataSupplier<M> implements StreamDataSupplier<M> {
	private static final Logger logger = Logger.getLogger("KafkaStreamDataSupplier");
	private org.apache.kafka.clients.consumer.Consumer<Void, M> consumer;
	private TopicPartition subscribedPartition;

	KafkaStreamDataSupplier(Properties properties, List<TopicPartition> partitions) {
		Validate.isTrue(partitions.size() == 1);
		subscribedPartition = partitions.get(0);
		consumer = new KafkaConsumer<Void, M>(properties);
		consumer.assign(partitions);
	}

	KafkaStreamDataSupplier(org.apache.kafka.clients.consumer.Consumer<Void, M> consumer,
			List<TopicPartition> partitions) {
		Validate.isTrue(partitions.size() == 1);
		subscribedPartition = partitions.get(0);
		this.consumer = consumer;
		consumer.assign(partitions);
	}

	@Override
	public String firstPosition() {
		long curPos = consumer.position(subscribedPartition);
		// move to beginning of the topic
		consumer.seekToBeginning(subscribedPartition);
		Long firstPos = consumer.position(subscribedPartition);
		consumer.seek(subscribedPartition, curPos);
		return firstPos.toString();
	}

	@Override
	public String firstPosition(Instant enqueuedAfter, Duration waitForArrival)
			throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String lastPosition() throws DataStreamInfrastructureException {
		long curPos = consumer.position(subscribedPartition);
		// move to end of the topic
		consumer.seekToEnd(subscribedPartition);
		long lastPos = consumer.position(subscribedPartition) - 1;
		consumer.seek(subscribedPartition, curPos);
		return String.valueOf(lastPos);
	}

	@Override
	public Instant enqueuedTime(String position) throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String nextStartPosition(String previousEndPosition) {
		Long nextPos = Long.parseLong(previousEndPosition) + 1;
		return nextPos.toString();
	}

	@Override
	public boolean isInRange(String position, String endPosition) {
		Validate.isTrue(position != null, "position cannot be null");
		if (endPosition == null) {
			return true;
		}
		return Long.parseLong(position) <= Long.parseLong(endPosition);
	}

	@Override
	public boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime) {
		// TODO throw exception. To change this throw exception will change lots
		// of interface.
		return false;
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		Long startPos = Long.parseLong(startPosition);
		Long endPos = Long.parseLong(endPosition);
		consumer.seek(subscribedPartition, startPos);
		consumer.position(subscribedPartition);
		long opMaxTime = System.currentTimeMillis() + timeoutDuration.toMillis();
		logger.log(Level.INFO,
				"maxTime=" + String.valueOf(opMaxTime) + ",now=" + String.valueOf(System.currentTimeMillis()));
		// lastPos is the last message start offset.
		long lastPos = 0;
		int count = 0;
		boolean outOfRange = false;
		while (true) {
			// Since poll can't specify maxItems, we just wait for timeout. We
			// can also do busy poll and check each return until maxItem reached
			// or timeout.
			long opNow = System.currentTimeMillis();
			ConsumerRecords<Void, M> records = consumer.poll(opMaxTime - opNow);
			logger.log(Level.INFO, "poll fetched " + records.count() + " messages");
			Iterator<ConsumerRecord<Void, M>> it = records.iterator();
			while (it.hasNext() && count < maxItems) {
				ConsumerRecord<Void, M> record = it.next();
				if (record.offset() > endPos) {
					outOfRange = true;
					return new SimpleReceiveStatus(String.valueOf(lastPos), null, outOfRange);
				}
				count++;
				list.add(record.value());
				lastPos = record.offset();
				logger.log(Level.INFO, "lastPos=" + String.valueOf(lastPos));
			}
			opNow = System.currentTimeMillis();
			if (opNow >= opMaxTime) {
				logger.log(Level.INFO, "timeout, lastPos=" + String.valueOf(lastPos));
				return new SimpleReceiveStatus(String.valueOf(lastPos), null, outOfRange);
			}
		}
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, String startPosition)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support startAsyncReceiving");
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, Instant startEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support startAsyncReceiving");
	}

	@Override
	public void stopAsyncReceiving(String id)  {
		return;
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, String endPosition)
			throws DataStreamInfrastructureException {
		long startPos = Long.parseLong(startPosition);
		long endPos = Long.parseLong(endPosition);
		consumer.seek(subscribedPartition, startPos);
		consumer.position(subscribedPartition);
		boolean outOfRange = false;
		long millisecondLeft = receiver.apply(null);
		long deadline = System.currentTimeMillis() + millisecondLeft;
		long lastPos = (long) -1;
		logger.log(Level.INFO, "receive called with startPos " + String.valueOf(startPos) + " endPos "
				+ String.valueOf(endPos) + " timeout:" + millisecondLeft);
		while (true) {
			// poll with max possible timeout specified by receiver.
			ConsumerRecords<Void, M> records = consumer.poll(millisecondLeft);
			Iterator<ConsumerRecord<Void, M>> it = records.iterator();
			logger.log(Level.INFO, "poll got " + records.count() + " records");
			while (it.hasNext()) {
				ConsumerRecord<Void, M> record = it.next();
				if (record.offset() <= endPos) {
					millisecondLeft = receiver.apply(record.value());
					lastPos = record.offset();
					if (millisecondLeft < 0) {
						return new SimpleReceiveStatus(String.valueOf(lastPos), null, outOfRange);
					}
				} else {
					outOfRange = true;
					return new SimpleReceiveStatus(String.valueOf(lastPos), null, outOfRange);
				}
			}
			if (System.currentTimeMillis() >= deadline) {
				return new SimpleReceiveStatus(String.valueOf(lastPos), null, outOfRange);
			}
			millisecondLeft = deadline - System.currentTimeMillis();
		}
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support Receive with start/end enqueue time");
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support receive with end enqueue time");
	}

	@Override
	public void start() throws Exception {
		return;
	}

	@Override
	public void stop() throws Exception {
		return;
	}

}
