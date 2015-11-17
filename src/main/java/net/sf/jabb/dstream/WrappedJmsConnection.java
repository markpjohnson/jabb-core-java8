/**
 * 
 */
package net.sf.jabb.dstream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import net.sf.jabb.util.jms.JmsUtility;
import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.WaitStrategy;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.qpid.amqp_1_0.client.ConnectionClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapped JMS Connection. The following features are added to the original JMS Connection:
 * <ul>
 * 	<li>It re-connects whenever got disconnected.</li>
 * 	<li>The stop() and start() methods are now thread-safe.</li>
 * </ul>
 * @author James Hu
 *
 */
public class WrappedJmsConnection implements Connection {
	static private final Logger logger = LoggerFactory.getLogger(WrappedJmsConnection.class);
	
	protected ConnectionFactory connectionFactory;
	protected volatile Connection connection;
	
	protected ExceptionListener exceptionListener;
	protected Predicate<Connection> connectionValidator;
	
	
	protected AtomicInteger stopStartLatch = new AtomicInteger(0);
	protected AtomicBoolean isConnecting = new AtomicBoolean(false);
	protected volatile int connectAttempts = 0;
	protected BackoffStrategy connectBackoffStrategy;
	protected WaitStrategy connectWaitStrategy;
	
	protected static ExecutorService threadPool;	// shared across all connections

	/**
	 * Constructor. Connection will be established immediately.
	 * @param connectionFactory			the factory for creating connections
	 * @param connectionValidator		validator returning true if a connection is considered to be valid
	 * @param connectBackoffStrategy	how to backoff between connect attempts	
	 * @param connectWaitStrategy		how to wait for the backoff
	 */
	public WrappedJmsConnection(ConnectionFactory connectionFactory, Predicate<Connection> connectionValidator,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy){
		this(connectionFactory, connectionValidator, connectBackoffStrategy, connectWaitStrategy, true);
	}
	
	/**
	 * Constructor
	 * @param connectionFactory			the factory for creating connections
	 * @param connectionValidator		validator returning true if a connection is considered to be valid
	 * @param connectBackoffStrategy	how to backoff between connect attempts	
	 * @param connectWaitStrategy		how to wait for the backoff
	 * @param connect					true if should connect now, false if no need to connect at this time
	 */
	public WrappedJmsConnection(ConnectionFactory connectionFactory, Predicate<Connection> connectionValidator,
			BackoffStrategy connectBackoffStrategy, WaitStrategy connectWaitStrategy, boolean connect){
		if (threadPool == null){
			synchronized(WrappedJmsConnection.class){
				if (threadPool == null){
					threadPool = new ThreadPoolExecutor(Integer.MAX_VALUE, Integer.MAX_VALUE, 2, TimeUnit.MINUTES,
							new LinkedBlockingQueue<>(),
							new BasicThreadFactory.Builder()
									.namingPattern(WrappedJmsConnection.class.getSimpleName() + "-%d")
									.priority(Thread.MIN_PRIORITY)
									.build());
					((ThreadPoolExecutor)threadPool).allowCoreThreadTimeOut(true);
				}
			}
		}
		
		this.connectionFactory = connectionFactory;
		this.connectionValidator = connectionValidator;
		this.connectBackoffStrategy = connectBackoffStrategy;
		this.connectWaitStrategy = connectWaitStrategy;
		
		this.exceptionListener = new ExceptionListener(){
			@Override
			public void onException(JMSException exception) {
				if (isConnectionClosed(exception)){
					threadPool.execute(()->establishConnection());
				}else{
					logger.debug("Connection related {}", JmsUtility.exceptionSummary(exception));
				}
			}
			
		};
		
		if (connect){
			establishConnection();
		}
	}
	
	/**
	 * Check if the exception is caused by connection closed/shutdown
	 * @param exception		the JMS exception
	 * @return		true if the exception is caused by connection closed/shutdown, false otherwise.
	 */
	protected boolean isConnectionClosed(JMSException exception){
		Exception linked = exception.getLinkedException();
		if(linked != null && linked instanceof ConnectionClosedException){
			return true;
		}
		String message = exception.getMessage();
		if (message != null && 
				(message.contains("class java.net.SocketException")
				|| message.contains("Connection has been shutdown") 
				|| message.contains("Connection closed by remote host")
				|| message.contains("The connection was inactive for more than the allowed period of time"))){
			return true;
		}
		return false;
	}
	
	/**
	 * Establish or re-establish connection in case it has been closed.
	 * It is thread-safe and has backoff between attempts.
	 * @return  true if a new connection established within this invocation, false otherwise
	 */
	public boolean establishConnection(){
		return establishConnection(true);
	}
	
	/**
	 * Is the connection being established?
	 * @return true if the connection is being established, false otherwise
	 */
	public boolean isConnecting(){
		return isConnecting.get();
	}
	
	/**
	 * Establish or re-establish connection in case it has been closed.
	 * It is thread-safe and can optionally apply backoff between attempts.
	 * @param	backoff   apply back off after failed attempt
	 * @return  true if a new connection established within this invocation, false otherwise
	 */
	public boolean establishConnection(boolean backoff){
		if (isConnecting.compareAndSet(false, true)){
			try{
				if (connection == null || !connectionValidator.test(connection)){
					if (connection != null){
						logger.debug("Connection closed: {}", connection);
					}
					connectAttempts ++;
					Connection newConn = null;
					try{
						newConn = connectionFactory.createConnection();
						if (connectionValidator.test(newConn)){
							newConn.setExceptionListener(exceptionListener);
						}else{
							closeSilently(newConn);
							newConn = null;
						}
					}catch(Exception e){
						logger.warn("Failed to establish new connection to replace closed one: {}. {}",
								connection,
								e instanceof JMSException? JmsUtility.exceptionSummary((JMSException) e) : "",
								e);
						closeSilently(newConn);
						newConn = null;
					}
					if (newConn != null){
						Connection oldConn = connection;
						synchronized(stopStartLatch){
							if (stopStartLatch.get() == 0){
								try {
									newConn.start();
								} catch (JMSException e) {
									logger.warn("Failed to start newly established connection: {}. {}",
											newConn, JmsUtility.exceptionSummary(e), e);
								}
							}
							connection = newConn;
						}
						if (connection == newConn){	// successfully (optionally) started and replaced
							logger.info("New connection {} established for replacing {}", newConn, oldConn);
							closeSilently(oldConn);
							connectAttempts = 0;
							return true;
						}else{
							closeSilently(newConn);
							newConn = null;
						}
					}
					
					// we arrived here because we failed to replace the old connection with a good new one
					if (backoff){
						try {
							connectWaitStrategy.await(connectBackoffStrategy.computeBackoffMilliseconds(connectAttempts));
						} catch (InterruptedException e1) {
							connectWaitStrategy.handleInterruptedException(e1);
						}
					}
				}
			}finally{
				isConnecting.set(false);
			}
		}else{
			// just ignore because someone else is handling it
		}
		return false;
	}
	
	/**
	 * Validate a connection by creating a consumer to a destination
	 * @param conn		the connection to be tested
	 * @param testConsumingDestination		the destination
	 * @return		true if a consumer can be successfully created; false otherwise.
	 */
	public static boolean validateConnectionByCreatingConsumer(Connection conn, Destination testConsumingDestination){
		Session session = null;
		MessageConsumer consumer = null;
		try {
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(testConsumingDestination);
		    return true;
		}catch(Exception e){
			logger.debug("Connection is not valid: {}", e instanceof JMSException ? JmsUtility.exceptionSummary((JMSException)e) : e.getMessage() );
			return false;
		}finally{
			closeSilently(consumer, session);
		}
	}

	/**
	 * Validate a connection by creating a producer to a destination
	 * @param conn		the connection to be tested
	 * @param testProducingDestination		the destination
	 * @return		true if a producer can be successfully created; false otherwise.
	 */
	public static boolean validateConnectionByCreatingProducer(Connection conn, Destination testProducingDestination){
		Session session = null;
		MessageProducer producer = null;
		try {
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			producer = session.createProducer(testProducingDestination);
		    return true;
		}catch(Exception e){
			logger.debug("Connection is not valid: {}", e instanceof JMSException ? JmsUtility.exceptionSummary((JMSException)e) : e.getMessage() );
			return false;
		}finally{
			closeSilently(producer, session);
		}
	}

	/**
	 * Get underlying connection
	 * @return	the original JMS connection created by the <code>ConnectionFactory</code>
	 */
	public Connection getConnection(){
		for(int i = 0; i < 10 && isConnecting(); i ++){
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		return connection;
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#createSession(boolean, int)
	 */
	@Override
	public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
		Connection conn = getConnection();
		if (conn == null){
			establishConnection(false);
		}
		try{
			return getConnection().createSession(transacted, acknowledgeMode);
		}catch(JMSException e){
			if (isConnectionClosed(e)){  // Event Hub closed the connection
				if (establishConnection(false)){
					return getConnection().createSession(transacted, acknowledgeMode);
				}
			}
			throw e;
		}
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#getClientID()
	 */
	@Override
	public String getClientID() throws JMSException {
		return getConnection().getClientID();
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#setClientID(java.lang.String)
	 */
	@Override
	public void setClientID(String clientID) throws JMSException {
		getConnection().setClientID(clientID);
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#getMetaData()
	 */
	@Override
	public ConnectionMetaData getMetaData() throws JMSException {
		return getConnection().getMetaData();
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#getExceptionListener()
	 */
	@Override
	public ExceptionListener getExceptionListener() throws JMSException {
		return getConnection().getExceptionListener();
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
	 */
	@Override
	public void setExceptionListener(ExceptionListener listener) throws JMSException {
		getConnection().setExceptionListener(listener);
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#start()
	 */
	@Override
	public void start() throws JMSException {
		synchronized(stopStartLatch){
			if (stopStartLatch.decrementAndGet() == 0){
				try{
					getConnection().start();
				}catch(JMSException | RuntimeException e){
					throw e;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#stop()
	 */
	@Override
	public void stop() throws JMSException {
		synchronized(stopStartLatch){
			if (stopStartLatch.getAndIncrement() == 0){
				try{
					getConnection().stop();
				}catch(JMSException | RuntimeException e){
					throw e;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#close()
	 */
	@Override
	public void close() throws JMSException {
		Connection conn = getConnection();
		if (conn != null){
			conn.close();
		}
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#createConnectionConsumer(javax.jms.Destination, java.lang.String, javax.jms.ServerSessionPool, int)
	 */
	@Override
	public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
			throws JMSException {
		return getConnection().createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages);
	}

	/* (non-Javadoc)
	 * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic, java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
	 */
	@Override
	public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector,
			ServerSessionPool sessionPool, int maxMessages) throws JMSException {
		return getConnection().createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
	}
	
	static protected void closeSilently(Connection conn){
		if (conn != null){
			try {
				conn.close();
			} catch (Exception e) {
				// ignore
			}
		}
	}

	
	static protected void closeSilently(MessageConsumer consumer, Session session){
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

	static protected void closeSilently(MessageProducer producer, Session session){
		if (producer != null){
			try{
				producer.close();
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
