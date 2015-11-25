/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.util.bean.DoubleValueBean;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.FunctionThrowsExceptions;
import net.sf.jabb.util.jms.JmsUtility;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.jgroups.util.UUID;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

/**
 * Template of StreamDataSupplier implementation based on JMS.
 * There is an assumption that <code>MessageConsumer</code> won't remove the received message.
 * Note: {@link #startAsyncReceiving(Consumer, String)} and {@link #stopAsyncReceiving(String)} should be called from the same thread
 * because the underlying <code>Session</code> and <code>MessageConsumer</code> objects are not thread safe.
 * 
 * @author James Hu
 * @param <M> type of the message object
 *
 */
abstract public class JmsConsumerStreamDataSupplier<M> implements StreamDataSupplier<M>{
	
	protected static TimeLimiter timeLimiter = new SimpleTimeLimiter(
			new ThreadPoolExecutor(0, Integer.MAX_VALUE,
					60L, TimeUnit.SECONDS,
					new SynchronousQueue<Runnable>(), 
					new BasicThreadFactory.Builder().namingPattern(JmsConsumerStreamDataSupplier.class.getSimpleName() + "-time-limiter-%d").build()));

	protected Queue destination;
	protected Map<String, DoubleValueBean<Session, MessageConsumer>> receivingConsumers = new ConcurrentHashMap<>();

	abstract protected Connection getConnection();
	abstract protected String messageSelector(String startPosition);
	abstract protected String messageSelector(Instant startEnqueuedTime);
	abstract protected M convert(Message message);
	abstract protected String position(Message message);
	abstract protected Instant enqueuedTime(Message message);
	
	protected ReceiveStatus fetch(List<? super M> list, Duration timeoutDuration, FunctionThrowsExceptions<java.util.Queue<Message>, Boolean> fetcher)  throws DataStreamInfrastructureException, InterruptedException{
		ConcurrentLinkedQueue<Message> fetched = new ConcurrentLinkedQueue<>();
		boolean outOfRangeReached = false;
		try{
			outOfRangeReached = fetcher.apply(fetched);
		}catch(TimeoutException|UncheckedTimeoutException e){
			// do nothing
		}catch(InterruptedException e){
			throw e;
		}catch(JMSException e){
			throw new DataStreamInfrastructureException(e);
		}catch(Exception e){
			throw Throwables.propagate(e);
		}
		
		int fetchedCount = fetched.size();		// the working thread may still being adding new element to fetched
		if (fetchedCount > 0){
			Message msg = null;
			while (fetchedCount-- > 0){
				msg = fetched.remove();
				list.add(convert(msg));
			}
			return new SimpleReceiveStatus(position(msg), enqueuedTime(msg), outOfRangeReached);
		}else{
			return new SimpleReceiveStatus(null, null, outOfRangeReached);
		}
	}
	
	protected ReceiveStatus fetch(List<? super M> list, String messageSelector, Predicate<Message> outOfRangeCheck, int maxItems, Duration timeoutDuration) throws DataStreamInfrastructureException, InterruptedException {
		return fetch(list, timeoutDuration, fetched->{
			long timeoutNano = System.nanoTime() + timeoutDuration.toNanos();
			long timeoutLeftMillis = (timeoutNano - System.nanoTime())/1000000;
			Session session = null;
			MessageConsumer consumer = null;
			boolean outOfRangeReached = false;
			try{
				session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
				consumer = session.createConsumer(destination, messageSelector);
				
				int count = 0;
				Message message = null;
				while (++count <= maxItems && timeoutLeftMillis > 0){
					message = consumer.receive(timeoutLeftMillis);
					if (message != null){
						if (outOfRangeCheck.test(message)){
							outOfRangeReached = true;
							break;
						}
						fetched.add(message);
					}
					timeoutLeftMillis = (timeoutNano - System.nanoTime())/1000000;
				}
			}finally{
				JmsUtility.closeSilently(consumer, session);
			}
			return outOfRangeReached;
		});
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, int maxItems, Duration timeoutDuration) throws DataStreamInfrastructureException, InterruptedException {
		return fetch(list, messageSelector(startPosition), 
				message -> endPosition != null && !isInRange(position(message), endPosition),
				maxItems, timeoutDuration);
	}
	
	@Override
	public ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration) throws DataStreamInfrastructureException, InterruptedException {
		return fetch(list, messageSelector(startEnqueuedTime), 
				message -> endEnqueuedTime != null && !isInRange(enqueuedTime(message), endEnqueuedTime),
				maxItems, timeoutDuration);
	}
	
	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration) throws DataStreamInfrastructureException, InterruptedException {
		return fetch(list, messageSelector(startPosition), 
				message -> endEnqueuedTime != null && !isInRange(enqueuedTime(message), endEnqueuedTime),
				maxItems, timeoutDuration);
	}
	
	protected String doStartAsyncReceiving(Consumer<M> objConsumer, String messageSelector) throws DataStreamInfrastructureException {
		String receivingConsumerId = UUID.randomUUID().toString();
		
		try{
			Connection connection = getConnection();
			connection.stop();
			try{
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageConsumer consumer = session.createConsumer(destination, messageSelector);
				consumer.setMessageListener(message -> objConsumer.accept(convert(message)));
				receivingConsumers.put(receivingConsumerId, new DoubleValueBean<>(session, consumer));
			}finally{
				connection.start();
			}
			
			return receivingConsumerId;
		}catch(JMSException e){
			throw new DataStreamInfrastructureException(e);
		}
	}



	@Override
	public String startAsyncReceiving(Consumer<M> objConsumer, String startPosition) throws DataStreamInfrastructureException {
		return doStartAsyncReceiving(objConsumer, messageSelector(startPosition));
	}

	@Override
	public String startAsyncReceiving(Consumer<M> objConsumer, Instant startEnqueuedTime) throws DataStreamInfrastructureException {
		return doStartAsyncReceiving(objConsumer, messageSelector(startEnqueuedTime));
	}

	@Override
	public void stopAsyncReceiving(String id) {
		DoubleValueBean<Session, MessageConsumer> receivingConsumer = receivingConsumers.remove(id);
		if (receivingConsumer != null){
			JmsUtility.closeSilently(receivingConsumer.getValue2(), receivingConsumer.getValue1());
		}
	}
	
	protected ReceiveStatus receive(Function<M, Long> receiver, String messageSelector, Predicate<Message> outOfRangeCheck) 
			throws DataStreamInfrastructureException{
		Session session = null;
		MessageConsumer consumer = null;
		boolean outOfRangeReached = false;
		try{
			session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(destination, messageSelector);
			
			long receiveTimeoutMillis = receiver.apply(null);
			Message message = null;
			Message lastMessage = null;
			while (receiveTimeoutMillis > 0){
				message = consumer.receive(receiveTimeoutMillis);
				if (message != null){
					if (outOfRangeCheck.test(message)){
						outOfRangeReached = true;
						break;
					}
					receiveTimeoutMillis = receiver.apply(convert(message));
					lastMessage = message;
				}else{
					receiveTimeoutMillis = receiver.apply(null);
				}
			}
			if (lastMessage != null){
				return new SimpleReceiveStatus(position(lastMessage), enqueuedTime(lastMessage), outOfRangeReached);
			}else{
				return new SimpleReceiveStatus(null, null, outOfRangeReached);
			}
		}catch(JMSException e){
			throw new DataStreamInfrastructureException(e);
		}finally{
			JmsUtility.closeSilently(consumer, session);
		}
	}
	
	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, String endPosition) 
			throws DataStreamInfrastructureException{
		return receive(receiver, messageSelector(startPosition), 
				message -> endPosition != null && !isInRange(position(message), endPosition));
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime) 
			throws DataStreamInfrastructureException{
		return receive(receiver, messageSelector(startEnqueuedTime), 
				message -> endEnqueuedTime != null && !isInRange(enqueuedTime(message), endEnqueuedTime));
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, Instant endEnqueuedTime) 
			throws DataStreamInfrastructureException{
		return receive(receiver, messageSelector(startPosition), 
				message -> endEnqueuedTime != null && !isInRange(enqueuedTime(message), endEnqueuedTime));
	}

}
