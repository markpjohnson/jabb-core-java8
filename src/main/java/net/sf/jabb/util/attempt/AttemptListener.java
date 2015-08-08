/**
 * 
 */
package net.sf.jabb.util.attempt;

/**
 * Listener of the events of finished attempts
 * @author James Hu
 *
 */
public interface AttemptListener {

	/**
	 * This method will be invoked right after an attempt finishes disregarding whether the attempt failed or succeeded.
	 * @param <R> type of the result of the attempt
	 * @param attempt	information about the attempt. 
	 * 			The context field of this object can be set and updated by the listener.
	 * 			The context object is shared across attempts.
	 */
	<R> void onAttempted(Attempt<R> attempt);
}
