/**
 * 
 */
package net.sf.jabb.taskq;

import java.io.Serializable;
import java.time.Instant;

/**
 * Read-only version of a task that is scheduled to be executed at a specific time.
 * @author James Hu
 *
 */
public interface ReadOnlyScheduledTask {
	/**
	 * Get the time that this task is expected to be executed
	 * @return	On client side, the result is always the time that this task is expected to be executed.
	 * 			On server side, if the task has never been dequeued, the result is the expectedExecution time specified when putting this task into the queue.
	 * 			On server side, if the task has been dequeued, the result is the next visible time of this task specified when getting this task from the queue.
	 */
	Instant getExpectedExecutionTime();
	
	/**
	 * Get the ID of the task that this task must run after it finishes. 
	 * If the predecessor cannot be found in the task queues, it is considered as already finished.
	 * @return	the ID of the predecessor task, or null if there is no predecessor.
	 */
	String getPredecessorId();
	
	/**
	 * Get the detail of the task
	 * @return full detail of the task
	 */
	Serializable getDetail();
	
	/**
	 * Get the number of times that this task had been dequeued/attempted
	 * @return the dequeue/attempt count
	 */
	int getAttempts();
	
	
	/**
	 * Get the ID of this task
	 * @return	the ID of this task
	 */
	String getTaskId();
	
}
