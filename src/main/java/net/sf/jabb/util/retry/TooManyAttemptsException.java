/**
 * 
 */
package net.sf.jabb.util.retry;

/**
 * Exception thrown in the situation that too many attempts has been performed and the stop strategy does not allow any further.
 * The exception happened during last attempt if exist, will be set as the cause.
 * @author James Hu
 *
 */
public class TooManyAttemptsException extends AttemptException {
	private static final long serialVersionUID = 341884244760490259L;

	public TooManyAttemptsException(Attempt<?> lastAttempt){
		super("Too many attempts: " + lastAttempt.getTotalAttempts());
		this.lastAttempt = lastAttempt;
		if (lastAttempt.hasException()){
			this.initCause(lastAttempt.getException());
		}
	}
	
}
