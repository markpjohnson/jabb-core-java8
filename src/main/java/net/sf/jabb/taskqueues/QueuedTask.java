/**
 * 
 */
package net.sf.jabb.taskqueues;

import java.io.Serializable;
import java.time.Instant;

/**
 * The task that had been put into a queue
 * @author James Hu
 *
 */
public interface QueuedTask {
	/**
	 * Get the time that this task is expected to be executed
	 * @return	On client side, the result is always the time that this task is expected to be executed.
	 * 			On server side, if the task has never been dequeued, the result is the expectedExecution time specified when putting this task into the queue.
	 * 			On server side, if the task has been dequeued, the result is the next visible time of this task specified when getting this task from the queue.
	 */
	Instant getExpectedExecutionTime();
	
	/**
	 * Get the ID of the task that this task must run after its finish
	 * @return	the ID of the predecessor task, or null if there is no predecessor.
	 */
	String getPredecessorId();
	
	/**
	 * Get the detail of the task
	 * @return full detail of the task
	 */
	Serializable getDetail();
	
	/**
	 * Get the time that this task was put into the queue
	 * @return the time that this task was put into the queue
	 */
	Instant getInsertionTime();
	
	/**
	 * Get the number of times that this task had been dequeued
	 * @return the dequeue count
	 */
	int getDequeueCount();
	
	/**
	 * Get the ID of the consumer that last dequeued this task
	 * @return ID of the last consumer
	 */
	String getLastConsumer();
	
	/**
	 * Get the ID of this enqueued task
	 * @return	the ID
	 */
	String getId();

}
