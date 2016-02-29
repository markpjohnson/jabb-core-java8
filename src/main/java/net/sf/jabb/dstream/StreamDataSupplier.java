/**
 * 
 */
package net.sf.jabb.dstream;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

/**
 * Abstraction of suppliers of stream data, such as Azure Event Hub, AWS Kinesis, Kafka, etc.
 * @author James Hu
 * @param <M> type of the message object
 */
public interface StreamDataSupplier<M> {
	
	/**
	 * Get the first position/offset in the stream. 
	 * This method can return a constant (e.g. "0", "", or null) representing the very first position if necessary.
	 * @return	the position/offset representing the first in the stream, can be null if necessary
	 */
	String firstPosition();
	
	/**
	 * Get the first position/offset in the stream enqueued after a specified time. 
	 * This method should never return a constant representing the very first position if necessary.
	 * @param enqueuedAfter		the time after which the the position of the first message needs to be returned
	 * @param waitForArrival  time duration to wait for arrival of the first message if it is not immediately available
	 * @return	the position/offset of the first data/message in the stream enqueued after the specified time, 
	 * 			or null if no message enqueued after the specified time can be found within the specified duration.
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened while finding out the first position meeting the condition in the stream
	 */
	String firstPosition(Instant enqueuedAfter, Duration waitForArrival) throws InterruptedException, DataStreamInfrastructureException;
	
	/**
	 * Get the first position/offset in the stream enqueued after a specified time. 
	 * This method wait for at most 2 seconds for the first message enqueued after that time to be available.
	 * This method should never return a constant representing the very first position if necessary.
	 * @param enqueuedAfter		the time after which the the position of the first message needs to be returned
	 * @return	the position/offset of the first data/message in the stream enqueued after the specified time, 
	 * 			or null if no message enqueued after the specified time can be found within 2 seconds.
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened while finding out the first position meeting the condition in the stream
	 */
	default String firstPosition(Instant enqueuedAfter) throws InterruptedException, DataStreamInfrastructureException{
		return firstPosition(enqueuedAfter, Duration.ofSeconds(15));
	}
	
	/**
	 * Get the last position/offset in the stream
	 * @return	the position/offset representing the last in the stream, or null if there is no message found.
	 * @throws  DataStreamInfrastructureException when error happened while finding out the last position in the stream
	 */
	String lastPosition() throws DataStreamInfrastructureException;
	
	/**
	 * Get the enqueued time of the message at a specified location
	 * @param position	the position of the message, the message must exist at this position
	 * @return			the time the message was enqueued
	 * @throws DataStreamInfrastructureException	if any error happened
	 */
	Instant enqueuedTime(String position) throws DataStreamInfrastructureException;
	
	/**
	 * Get the next start position/offset
	 * @param previousEndPosition		previous end position/offset. 
	 * @return	the next start position/offset that can ensure there is no gap between it and previousEndPosition
	 */
	String nextStartPosition(String previousEndPosition);
	
	/**
	 * Check if a position is within the range defined by an end position
	 * @param position		the position to be checked
	 * @param endPosition	the end position
	 * @return	true if in range, false otherwise
	 */
	boolean isInRange(String position, String endPosition);
	
	/**
	 * Check if an enqueued time is within the range defined by an end enqueued
	 * time
	 * 
	 * @param enqueuedTime
	 *            the enqueued time to be checked
	 * @param endEnqueuedTime
	 *            the end enqueued time
	 * @return true if in range, false otherwise
	 */
	boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime);
	
	/**
	 * Fetch the data/messages in the range specified by start and end positions.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endPosition		the end position, inclusive/exclusive defined by the implementation
	 * @param maxItems			maximum number of items that will be fetched and added into the list
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, int maxItems, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException;

	/**
	 * Fetch the data/messages in the range specified by start and end positions, allowing fetching as much data as possible.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endPosition		the end position, inclusive/exclusive defined by the implementation
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	default ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException{
		return fetch(list, startPosition, endPosition, Integer.MAX_VALUE, timeoutDuration);
	}

	/**
	 * Fetch the data/messages starting from a position
	 * @param list				list into which the data/messages will be added
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param maxItems			maximum number of items that will be fetched and added into the list
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	default ReceiveStatus fetch(List<? super M> list, String startPosition, int maxItems, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException{
		return fetch(list, startPosition, (String)null, maxItems, timeoutDuration);
	}
	
	/**
	 * Fetch the data/messages in the range specified by start and end enqueued time.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startEnqueuedTime		the start enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param maxItems			maximum number of items that will be fetched and added into the list
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException;

	/**
	 * Fetch the data/messages in the range specified by start and end positions, allowing fetching as much data as possible.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startEnqueuedTime		the start enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	default ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException{
		return fetch(list, startEnqueuedTime, endEnqueuedTime, Integer.MAX_VALUE, timeoutDuration);
	}

	/**
	 * Fetch the data/messages starting from a position
	 * @param list				list into which the data/messages will be added
	 * @param startEnqueuedTime		the start enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param maxItems			maximum number of items that will be fetched and added into the list
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	default ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, int maxItems, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException{
		return fetch(list, startEnqueuedTime, null, maxItems, timeoutDuration);
	}
	
	/**
	 * Fetch the data/messages in the range specified by start position and end enqueued time.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param maxItems			maximum number of items that will be fetched and added into the list
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException;

	/**
	 * Fetch the data/messages in the range specified by start position and end positions, allowing fetching as much data as possible.
	 * @param list		list into which the data/messages found within the range will be added
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param timeoutDuration	maximum total duration allowed for fetch those data
	 * @return	ReceiveStatus with position of the last message added into the list or null if no message had been added
	 * @throws  InterruptedException if the thread is interrupted
	 * @throws  DataStreamInfrastructureException  when error happened
	 */
	default ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException{
		return fetch(list, startPosition, endEnqueuedTime, Integer.MAX_VALUE, timeoutDuration);
	}


	
	/**
	 * Start receiving data/messages starting from a position asynchronously. 
	 * The method of the receiver will be called from a background thread, rather than the calling thread of this method.
	 * @param receiver			the receiver of the data/messages
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @return				an ID for this receiving session
	 * @throws DataStreamInfrastructureException 	any exception
	 */
	String startAsyncReceiving(Consumer<M> receiver, String startPosition) throws DataStreamInfrastructureException;
	
	/**
	 * Start receiving data/messages starting from an enqueued time asynchronously. 
	 * The method of the receiver will be called from a background thread, rather than the calling thread of this method.
	 * @param receiver			the receiver of the data/messages
	 * @param startEnqueuedTime		the start enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @return				an ID for this receiving session
	 * @throws DataStreamInfrastructureException 	any exception
	 */
	String startAsyncReceiving(Consumer<M> receiver, Instant startEnqueuedTime) throws DataStreamInfrastructureException;
	
	/**
	 * Stop asynchronous receiving
	 * @param id			ID of the receiving session
	 */
	void stopAsyncReceiving(String id);
	
	/**
	 * Synchronously receive data/messages starting from a position and ending before another position.
	 * The method of the receiver will be called from the calling thread of this method.
	 * @param receiver			The receiver which accepts one message each time and return number of milliseconds left for receiving remaining next message
	 * 							If the receiver receives a null as input, it should ignore it but still return the correct number of milliseconds left.
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endPosition		the end position, inclusive/exclusive defined by the implementation
	 * @return	ReceiveStatus with position of the last message received or null if no message had been received
	 * @throws DataStreamInfrastructureException	if any error happened
	 */
	ReceiveStatus receive(Function<M, Long> receiver, String startPosition, String endPosition) throws DataStreamInfrastructureException;
	
	/**
	 * Synchronously receive data/messages starting from an enqueued time and ending before another enqueued time.
	 * The method of the receiver will be called from the calling thread of this method.
	 * @param receiver			The receiver which accepts one message each time and return number of milliseconds left for receiving remaining next message
	 * 							If the receiver receives a null as input, it should ignore it but still return the correct number of milliseconds left.
	 * @param startEnqueuedTime		the start enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @return	ReceiveStatus with position of the last message received or null if no message had been received
	 * @throws DataStreamInfrastructureException	if any error happened
	 */
	ReceiveStatus receive(Function<M, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime) throws DataStreamInfrastructureException;
	
	/**
	 * Synchronously receive data/messages starting from a position and ending before another enqueued time.
	 * The method of the receiver will be called from the calling thread of this method.
	 * @param receiver			The receiver which accepts one message each time and return number of milliseconds left for receiving remaining next message
	 * 							If the receiver receives a null as input, it should ignore it but still return the correct number of milliseconds left.
	 * @param startPosition		the start position, inclusive/exclusive defined by the implementation
	 * @param endEnqueuedTime		the end enqueued time of the message/data, inclusive/exclusive defined by the implementation
	 * @return	ReceiveStatus with position of the last message received or null if no message had been received
	 * @throws DataStreamInfrastructureException	if any error happened
	 */
	ReceiveStatus receive(Function<M, Long> receiver, String startPosition, Instant endEnqueuedTime) throws DataStreamInfrastructureException;
	
	/**
	 * Start/activate/connect to the data stream
	 * @throws Exception any exception
	 */
	void start() throws Exception;
	
	/**
	 * Stop/deactivate/disconnect from the data stream
	 * @throws Exception any exception
	 */
	void stop() throws Exception;
	
	/**
	 * Wrap this and an id into a <code>StreamDataSupplierWithIdImpl</code> data structure
	 * @param id	ID of the supplier
	 * @return		an <code>StreamDataSupplierWithIdImpl</code> instance
	 */
	default StreamDataSupplierWithId<M> withId(String id){
		return new StreamDataSupplierWithIdImpl<>(id, this);
	}
	
}
