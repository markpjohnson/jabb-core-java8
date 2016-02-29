/**
 * 
 */
package net.sf.jabb.dstream.eventhub;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.StreamDataSupplierWithIdImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.UncheckedTimeoutException;

import net.sf.jabb.azure.AzureEventHubUtility;
import net.sf.jabb.azure.EventHubAnnotations;
import net.sf.jabb.dstream.JmsConsumerStreamDataSupplier;
import net.sf.jabb.dstream.WrappedJmsConnection;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.util.jms.JmsUtility;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.Sequencer;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.parallel.WaitStrategy;
import net.sf.jabb.util.text.DurationFormatter;

/**
 * Stream data supplier for accessing messages in Azure Event Hub through Qpid JMS.
 * It defines position range as (startPosition, endPosition].
 * It is assumed that x-opt-offset is a Long number.
 * @author James Hu
 *
 */
public class EventHubQpidStreamDataSupplier<M> extends JmsConsumerStreamDataSupplier<M> {
	private static final Logger logger = LoggerFactory.getLogger(EventHubQpidStreamDataSupplier.class);
	
	protected Function<Message, M> messageConverter;
	
	protected WrappedJmsConnection wrappedConnection;
	
	protected static final Sequencer idSequencer = new Sequencer();
	
	protected String identifier;
	
	protected WaitStrategy waitStrategy;
	
	/**
	 * Constructor
	 * @param connectionFactory			JMS connection factory
	 * @param destination				JMS destination
	 * @param connectionValidator		predicate for validating JMS connection
	 * @param connectBackoffStrategy	backoff strategy for retrying connection establishment
	 * @param waitStrategy				wait strategy for retrying connection establishment and other operations
	 * @param messageConverter			converter for converting JMS messages
	 */
	public EventHubQpidStreamDataSupplier(ConnectionFactory connectionFactory, Queue destination, Predicate<Connection> connectionValidator,
			BackoffStrategy connectBackoffStrategy, WaitStrategy waitStrategy,
			Function<Message, M> messageConverter){
		Validate.notNull(connectionFactory, "connection factory cannot be null");
		Validate.notNull(destination, "destination cannot be null");
		Validate.notNull(connectionValidator, "connection validator cannot be null");
		Validate.notNull(connectBackoffStrategy, "connect backoff strategy cannot be null");
		Validate.notNull(waitStrategy, "wait strategy cannot be null");
		Validate.notNull(messageConverter, "message converter cannot be null");
		
		this.waitStrategy = waitStrategy;
		this.messageConverter = messageConverter;
		this.destination = destination;
		try {
			this.identifier = connectionFactory.toString() + "->" + destination.getQueueName();
		} catch (JMSException e) {
			this.identifier = connectionFactory.toString() + "->UNKNOWN";
		}
		
		this.wrappedConnection = new WrappedJmsConnection(connectionFactory, connectionValidator, connectBackoffStrategy, waitStrategy, false);
	}
	
	/**
	 * Constructor with default backoff strategy and wait strategy: 
	 * BackoffStrategies.fibonacciBackoff(1000, 20, TimeUnit.SECONDS), WaitStrategies.threadSleepStrategy()
	 * 
	 * @param host						something like your_name_space.servicebus.windows.net
	 * @param eventHubName				name of the Event Hub
	 * @param policyName				something like ReceiveRule
	 * @param policyKey					the key of the policy
	 * @param consumerGroup				consumer gorup, or null for the default one '$Default'
	 * @param partition					partition number
	 * @param messageConverter			converter for converting JMS messages
	 */
	public EventHubQpidStreamDataSupplier(String host, String eventHubName, String policyName, String policyKey, 
			String consumerGroup, String partition, Function<Message, M> messageConverter){
		this(host, eventHubName, policyName, policyKey, consumerGroup, partition, 
				BackoffStrategies.fibonacciBackoff(1000, 20, TimeUnit.SECONDS), 
				WaitStrategies.threadSleepStrategy(),
				messageConverter);
	}
	
	/**
	 * Constructor with default backoff strategy and wait strategy: 
	 * BackoffStrategies.fibonacciBackoff(1000, 20, TimeUnit.SECONDS), WaitStrategies.threadSleepStrategy()
	 * 
	 * @param host						something like your_name_space.servicebus.windows.net
	 * @param eventHubName				name of the Event Hub
	 * @param policyName				something like ReceiveRule
	 * @param policyKey					the key of the policy
	 * @param consumerGroup				consumer gorup, or null for the default one '$Default'
	 * @param partition					partition number
	 * @param messageConverter			converter for converting JMS messages
	 */
	public EventHubQpidStreamDataSupplier(String host, String eventHubName, String policyName, String policyKey, 
			String consumerGroup, int partition, Function<Message, M> messageConverter){
		this(host, eventHubName, policyName, policyKey, consumerGroup, String.valueOf(partition), 
				BackoffStrategies.fibonacciBackoff(1000, 20, TimeUnit.SECONDS), 
				WaitStrategies.threadSleepStrategy(),
				messageConverter);
	}
	
	/**
	 * Constructor
	 * @param server						something like your_name_space.servicebus.windows.net
	 * @param eventHubName				name of the Event Hub
	 * @param policyName				something like ReceiveRule
	 * @param policyKey					the key of the policy
	 * @param consumerGroup				consumer gorup, or null for the default one '$Default'
	 * @param partition					partition number
	 * @param connectBackoffStrategy	backoff strategy for retrying connection establishment
	 * @param connectWaitStrategy		wait strategy for retrying connection establishment
	 * @param messageConverter			converter for converting JMS messages
	 */
	public EventHubQpidStreamDataSupplier(String server, String eventHubName, String policyName, String policyKey, 
			String consumerGroup, int partition,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy, Function<Message, M> messageConverter){
		this(server, eventHubName, policyName, policyKey, consumerGroup, String.valueOf(partition),
				connectBackoffStrategy, connectWaitStrategy, messageConverter);
	}
	
	/**
	 * Constructor
	 * @param server						something like your_name_space.servicebus.windows.net
	 * @param eventHubName				name of the Event Hub
	 * @param policyName				something like ReceiveRule
	 * @param policyKey					the key of the policy
	 * @param consumerGroup				consumer gorup, or null for the default one '$Default'
	 * @param partition					partition number
	 * @param connectBackoffStrategy	backoff strategy for retrying connection establishment
	 * @param connectWaitStrategy		wait strategy for retrying connection establishment
	 * @param messageConverter			converter for converting JMS messages
	 */
	public EventHubQpidStreamDataSupplier(String server, String eventHubName, String policyName, String policyKey, 
			String consumerGroup, String partition,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy, Function<Message, M> messageConverter){
		Validate.notBlank(server, "server cannot be blank");
		Validate.notBlank(eventHubName, "Event Hub name cannot be blank");
		Validate.notBlank(policyName, "access policy name cannot be blank");
		Validate.notBlank(policyKey, "access policy key cannot be blank");
		Validate.notNull(connectBackoffStrategy, "connect backoff strategy cannot be null");
		Validate.notNull(connectWaitStrategy, "connect wait strategy cannot be null");
		Validate.notNull(messageConverter, "message converter cannot be null");
		
		String clientId =  makeClientId();

		this.messageConverter = messageConverter;
		this.destination = createQueue(eventHubName, consumerGroup, partition);
		this.identifier = clientId + "->" + policyName + ":" + server + "/" + eventHubName + "/" + consumerGroup + "/" + partition;
		this.wrappedConnection = createConnectionForReceiving(server, policyName, policyKey, destination,
				connectBackoffStrategy, connectWaitStrategy);
	}
	
	/**
	 * Create a list of {@link StreamDataSupplierWithIdImpl}s from an Event Hub.
	 * @param <M>		type of the message
	 * @param server		the server name containing name space of the Event Hub
	 * @param policyName	policy with read permission
	 * @param policyKey		key of the policy
	 * @param eventHubName	name of the Event Hub
	 * @param consumerGroup		consumer group name
	 * @param messageConverter	JMS message converter
	 * @return					a list of {@link StreamDataSupplierWithIdImpl}s, one per partition
	 * @throws JMSException		If list of partitions cannot be fetched
	 */
	public static <M> List<StreamDataSupplierWithId<M>> create(String server, String policyName, String policyKey,
	                                                           String eventHubName, String consumerGroup, Function<Message, M> messageConverter) throws JMSException{
		String[] partitions = AzureEventHubUtility.getPartitions(server, policyName, policyKey, eventHubName);
		List<StreamDataSupplierWithId<M>> suppliers = new ArrayList<>(partitions.length);
		for (String partition: partitions){
			EventHubQpidStreamDataSupplier<M> supplier = new EventHubQpidStreamDataSupplier<>(server, eventHubName, policyName, policyKey,
					consumerGroup, partition, messageConverter);
			suppliers.add(new StreamDataSupplierWithIdImpl<>(partition, supplier));
		}
		return suppliers;
	}
	
	static protected String makeClientId(){
		return EventHubQpidStreamDataSupplier.class.getSimpleName() + "-" + idSequencer.next() + "@" + getHostName();
	}

	static protected String getHostName(){
		String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName().toLowerCase();
        } catch (UnknownHostException e) {
            hostname = "<HOSTNAME UNKNOWN>";
        }
        return hostname;
	}
	
	static public ConnectionFactory createConnectionFactory(String server, String policyName, String policyKey){
		return createConnectionFactory(server, policyName, policyKey, null);
	}
	
	/**
	 * Create a connection factory
	 * @param server		server domain name or ip address
	 * @param policyName	policy name
	 * @param policyKey		policy key
	 * @param eventHubName	name of the event hub, will only be used in log messages
	 * @return	the connection factory
	 */
	static public ConnectionFactory createConnectionFactory(String server, String policyName, String policyKey, String eventHubName){
		EventHubQpidConnectionFactory connectionFactory = new EventHubQpidConnectionFactory(
				"amqps", server, 5671, policyName, policyKey, makeClientId(), 
				server, true, 0, eventHubName);
		return connectionFactory;
	}
	
	static public WrappedJmsConnection createConnection(String server, String policyName, String policyKey, 
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy,
			Predicate<Connection> connectionValidator, String eventHubName){
		ConnectionFactory connectionFactory = createConnectionFactory(server, policyName, policyKey, eventHubName);
		
		return new WrappedJmsConnection(connectionFactory, 
				connectionValidator, 
				connectBackoffStrategy, 
				connectWaitStrategy,
				false);
	}
	
	static public WrappedJmsConnection createConnection(String server, String policyName, String policyKey, 
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy,
			Predicate<Connection> connectionValidator){
		return createConnection(server, policyName, policyKey, connectBackoffStrategy, connectWaitStrategy, connectionValidator, null);
	}
	
	static public WrappedJmsConnection createConnectionForReceiving(String server, String policyName, String policyKey, 
			Destination eventHub,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy){
			return createConnection(server, policyName, policyKey, connectBackoffStrategy, connectWaitStrategy, 
					conn -> WrappedJmsConnection.validateConnectionByCreatingConsumer(conn, eventHub), getQueueName(eventHub));
	}
	
	static public WrappedJmsConnection createConnectionForSending(String server, String policyName, String policyKey, 
			Destination eventHub,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy,
			String clientId){
		return createConnection(server, policyName, policyKey, connectBackoffStrategy, connectWaitStrategy, 
				conn -> WrappedJmsConnection.validateConnectionByCreatingProducer(conn, eventHub), getQueueName(eventHub));
	}
	
	static public String getQueueName(Destination d){
		if (d instanceof Queue){
			try {
				return ((Queue)d).getQueueName();
			} catch (JMSException e) {
				throw Throwables.propagate(e);
			}
		}else{
			return d.toString();
		}
	}
	
	static public Queue createQueue(String eventHubName, String consumerGroup, String partition){
		org.apache.qpid.amqp_1_0.jms.impl.QueueImpl eventHub = org.apache.qpid.amqp_1_0.jms.impl.QueueImpl.valueOf(
				eventHubName + "/ConsumerGroups/" + (consumerGroup == null ? "$Default" : consumerGroup) + "/Partitions/" + partition);
		return eventHub;
	}

	static public Queue createQueue(String eventHubName, String consumerGroup, int partition){
		return createQueue(eventHubName, consumerGroup, String.valueOf(partition));
	}

	static public Queue createQueue(String eventHubName){
		org.apache.qpid.amqp_1_0.jms.impl.QueueImpl eventHub = org.apache.qpid.amqp_1_0.jms.impl.QueueImpl.valueOf(
				eventHubName);
		return eventHub;
	}

	/**
	 *  {@inheritDoc}
	 *  This method always returns -1.
	 */
	@Override
	public String firstPosition() {
		return String.valueOf(-1);
	}

	@Override
	public Instant enqueuedTime(String position) throws DataStreamInfrastructureException{
		String selector = "amqp.annotation.x-opt-offset >= '" + position + "'";
		
		Message msg = null;
		try {
			msg = firstMessageByReceive(selector, 0);
			if (msg == null){
				logger.warn("Null message received for position {}", position);
				return null;
			}
			EventHubAnnotations annotations = AzureEventHubUtility.getEventHubAnnotations(msg);
			return annotations.getEnqueuedTime();
		} catch (JMSException | InterruptedException e) {
			throw new DataStreamInfrastructureException(e);
		}
	}


	/**
	 * {@inheritDoc}
	 * This method returns  (offset of the first message after (exclusive) <code>enqueuedAfter</code>) - 1 if there is at least a message after <code>enquedTimeNoEarlyThan</code>.
	 * Null is returned if there is no message enqueued after <code>enqueuedAfter</code>
	 * @throws InterruptedException 	if got interrupted
	 * @throws DataStreamInfrastructureException				if any JMS level error happened
	 */
	@Override
	public String firstPosition(Instant enqueuedAfter, Duration waitForArrival) throws DataStreamInfrastructureException, InterruptedException{
		long opStartTime = System.currentTimeMillis();
		long enqueuedAfterEpochMillis = enqueuedAfter.toEpochMilli();
		long waitForArrivalMillis = waitForArrival.toMillis();
		String selector = "amqp.annotation.x-opt-enqueued-time > '" + enqueuedAfterEpochMillis + "'";
		
		Message msg;
		try {
			msg = firstMessageByReceive(selector, waitForArrivalMillis);
		} catch (JMSException e) {
			throw new DataStreamInfrastructureException(e);
		}
		//msg = firstMessageByListener(selector, waitForArrivalMillis);
		//msg = firstMessageByBrowser(selector, waitForArrivalMillis);
		
		EventHubAnnotations annotations = null;
		String firstPosition = null;
		if (msg != null){
			annotations = AzureEventHubUtility.getEventHubAnnotations(msg);
			firstPosition = String.valueOf(annotations.getOffset() - 1);
		}
		logger.debug("First position in {} after {} identified within {}: {}", identifier, enqueuedAfter, DurationFormatter.formatSince(opStartTime), annotations);
		return firstPosition;
	}

	protected Message firstMessageByListener(String selector, long waitForArrivalMillis) throws JMSException, InterruptedException{
		AtomicReference<Message> result = new AtomicReference<>(null);
		AtomicBoolean gotIt = new AtomicBoolean(false);
		CountDownLatch stillWaiting = new CountDownLatch(1);
		
		Session session = null;
		MessageConsumer consumer = null;
		try{
			Connection conn = getConnection();
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			conn.stop();
			try{
				consumer = session.createConsumer(destination, selector);
				consumer.setMessageListener(msg->{
					if (gotIt.compareAndSet(false, true)){
						result.set(msg);
						stillWaiting.countDown();
					}
				});
			}finally{
				conn.start();
			}
			stillWaiting.await(waitForArrivalMillis, TimeUnit.MILLISECONDS);
		}finally{
			JmsUtility.closeSilently(consumer, session);
		}

		return result.get();
	}

	protected Message firstMessageByReceive(String selector, long waitForArrivalMillis) throws JMSException, InterruptedException{
		Session session = null;
		MessageConsumer consumer = null;
		try{
			Connection conn = getConnection();
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(destination, selector);
			Message msg = null;
			long waitTime = waitForArrivalMillis > 0 ? waitForArrivalMillis : Long.MAX_VALUE;
			long startTime = System.currentTimeMillis();
			while(msg == null && waitTime > 0){		// sometimes the consumer is not fully ready immediatly
				msg = consumer.receive(waitTime);
				waitTime -= System.currentTimeMillis() - startTime;
			}
			return msg;
		}finally{
			JmsUtility.closeSilently(consumer, session);
		}
	}

	protected Message firstMessageByBrowser(String selector, long waitForArrivalMillis) throws JMSException, InterruptedException{
		Session session = null;
		QueueBrowser browser = null;
		try{
			Connection conn = getConnection();
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			browser = session.createBrowser(destination, selector);
			@SuppressWarnings("rawtypes")
			Enumeration enumeration = browser.getEnumeration();
			Message msg = null;
			try{
				msg =  timeLimiter.callWithTimeout(()->{
					if(enumeration.hasMoreElements()){
						return (Message) enumeration.nextElement();
					}else{
						return null;
					}
				},waitForArrivalMillis, TimeUnit.MILLISECONDS, true);
			}catch(UncheckedTimeoutException  e){
				// ignore
			}catch(JMSException e){
				throw e;
			}catch(Exception e){
				throw Throwables.propagate(e);
			}
			return msg;
		}finally{
			JmsUtility.closeSilently(browser, session);
		}
	}


	/* (non-Javadoc)
	 * @see net.sf.jabb.stream.DataStreamProvider#lastPosition()
	 */
	@Override
	public String lastPosition() throws DataStreamInfrastructureException {
		long opStartTime = System.currentTimeMillis();
		
		Long lastPosition;
		try {
			lastPosition = lastPositionByTryError();
		} catch (JMSException e) {
			throw new DataStreamInfrastructureException(e);
		}
		//lastPosition = lastPositionThroughManagementQueue();
		
		logger.debug("Last position in {} identified within {}: {}", identifier, DurationFormatter.formatSince(opStartTime), lastPosition);
		return lastPosition == null ? null : lastPosition.toString();
	}
	
	protected Long lastPositionByTryError() throws JMSException {
		Long lastPosition = null;
		
		Session session = null;
		MessageConsumer consumer = null;
		try{
			session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
			try{
				consumer = session.createConsumer(destination, "amqp.annotation.x-opt-offset >= '" + (Long.MAX_VALUE/2 - 1) + "'");
			}catch(JMSException e){
				Throwable cause = e.getCause();
				if (cause != null && cause.getMessage() != null){
					String msg = cause.getMessage();
					String LEADING_TEXT = "The last offset in the system is ";
					int i = msg.indexOf(LEADING_TEXT);
					if (i >= 0){
						i += LEADING_TEXT.length();
						int start = i;
						i = msg.indexOf(' ', start + 1);
						if (i >= 0){
							lastPosition = Long.valueOf(
									StringUtils.removeEnd(StringUtils.removeStart(StringUtils.trimToNull(msg.substring(start, i)), "'"), "'"));
						}
					}
				}
				if (lastPosition == null){	// if we cannot find it, that's an exception
					throw e;
				}
			}
		}finally{
			JmsUtility.closeSilently(consumer, session);
		}

		return lastPosition;
	}
	
	@Override
	public String nextStartPosition(String position) {
		return position;
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.stream.AbstractJmsDataStreamProvider#getConnection()
	 */
	@Override
	protected Connection getConnection() {
		return wrappedConnection;
	}

	@Override
	protected String messageSelector(String startPosition) {
		return "amqp.annotation.x-opt-offset > '" + startPosition + "'";
	}

	@Override
	protected String messageSelector(Instant startEnqueuedTime) {
		return "amqp.annotation.x-opt-enqueued-time > '" + startEnqueuedTime.toEpochMilli() + "'";
	}

	@Override
	public boolean isInRange(String position, String endPosition) {
		Validate.isTrue(position != null, "position cannot be null");
		if (endPosition == null){
			return true;
		}else{
			return Long.parseLong(position) <= Long.parseLong(endPosition);
		}
	}
	
	@Override
	public boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime) {
		Validate.isTrue(enqueuedTime != null, "enqueuedTime cannot be null");
		if (endEnqueuedTime == null){
			return true;
		}else{
			return !enqueuedTime.isAfter(endEnqueuedTime);
		}
	}


	/* (non-Javadoc)
	 * @see net.sf.jabb.stream.AbstractJmsDataStreamProvider#convert(javax.jms.Message)
	 */
	@Override
	protected M convert(Message message) {
		return messageConverter.apply(message);
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.stream.AbstractJmsDataStreamProvider#position(javax.jms.Message)
	 */
	@Override
	protected String position(Message message) {
		return String.valueOf(AzureEventHubUtility.getEventHubAnnotations(message).getOffset());
	}
	
	@Override
	protected Instant enqueuedTime(Message message) {
		return AzureEventHubUtility.getEventHubAnnotations(message).getEnqueuedTime();
	}

	@Override
	public void start() throws Exception {
		long opStartTime = System.currentTimeMillis();
		wrappedConnection.establishConnection();
		while(System.currentTimeMillis() < opStartTime + 60*1000L){
			if (!wrappedConnection.isConnecting()){
				return;
			}
			try{
				waitStrategy.await(100);
			}catch(InterruptedException e){
				waitStrategy.handleInterruptedException(e);
				break;
			}
		}
		logger.warn("Connection is still not established after start: {}", DurationFormatter.formatSince(opStartTime));
		return;
	}

	@Override
	public void stop() throws Exception {
		// TODO: more work needed to define the behaviour of start/stop/restart
		// for now, there is no support for restart
		wrappedConnection.close();
	}

}
