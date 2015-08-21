/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Duration;
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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.util.bean.DoubleValueBean;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.ConsumerThrowsExceptions;
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
	abstract protected M convert(Message message);
	abstract protected String position(Message message);
	
	protected String fetch(List<? super M> list, Duration timeoutDuration, ConsumerThrowsExceptions<java.util.Queue<Message>> fetcher)  throws DataStreamInfrastructureException, InterruptedException{
		ConcurrentLinkedQueue<Message> fetched = new ConcurrentLinkedQueue<>();
		try{
			//timeLimiter.callWithTimeout(()->{
				fetcher.accept(fetched);
			//	return null;
			//}, timeoutDuration.toMillis(), TimeUnit.MILLISECONDS, true);
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
			return position(msg);
		}else{
			return null;
		}
	}
	
	@Override
	public String fetch(List<? super M> list, String startPosition, String endPosition, int maxItems, Duration timeoutDuration) throws DataStreamInfrastructureException, InterruptedException {
		return fetch(list, timeoutDuration, fetched->{
			long timeoutNano = System.nanoTime() + timeoutDuration.toNanos();
			long timeoutLeftMillis = (timeoutNano - System.nanoTime())/1000000;
			Session session = null;
			MessageConsumer consumer = null;
			try{
				session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
				consumer = session.createConsumer(destination, messageSelector(startPosition));
				
				int count = 0;
				Message message = null;
				while (++count <= maxItems && timeoutLeftMillis > 0){
					message = consumer.receive(timeoutLeftMillis);
					if (message != null){
						if (endPosition != null && !isInRange(position(message), endPosition)){
							break;
						}
						fetched.add(message);
					}
					timeoutLeftMillis = (timeoutNano - System.nanoTime())/1000000;
				}
			}finally{
				JmsUtility.closeSilently(consumer, session);
			}
		});
	}

	@Override
	public String startAsyncReceiving(Consumer<M> objConsumer, String startPosition) throws DataStreamInfrastructureException {
		String receivingConsumerId = UUID.randomUUID().toString();
		
		try{
			Connection connection = getConnection();
			connection.stop();
			try{
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageConsumer consumer = session.createConsumer(destination, messageSelector(startPosition));
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
	public void stopAsyncReceiving(String id) {
		DoubleValueBean<Session, MessageConsumer> receivingConsumer = receivingConsumers.remove(id);
		if (receivingConsumer != null){
			JmsUtility.closeSilently(receivingConsumer.getValue2(), receivingConsumer.getValue1());
		}
	}
	
	@Override
	public String receive(Function<M, Long> receiver, String startPosition, String endPosition) 
			throws DataStreamInfrastructureException{
		Session session = null;
		MessageConsumer consumer = null;
		try{
			session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(destination, messageSelector(startPosition));
			
			long receiveTimeoutMillis = receiver.apply(null);
			Message message = null;
			Message lastMessage = null;
			while (receiveTimeoutMillis > 0){
				message = consumer.receive(receiveTimeoutMillis);
				if (message != null){
					if (endPosition != null && !isInRange(position(message), endPosition)){
						break;
					}
					receiveTimeoutMillis = receiver.apply(convert(message));
					lastMessage = message;
				}else{
					receiveTimeoutMillis = receiver.apply(null);
				}
			}
			return lastMessage == null ? null : position(lastMessage);
		}catch(JMSException e){
			throw new DataStreamInfrastructureException(e);
		}finally{
			JmsUtility.closeSilently(consumer, session);
		}
	}

}
