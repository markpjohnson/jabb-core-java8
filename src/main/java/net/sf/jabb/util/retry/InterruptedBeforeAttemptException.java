/**
 * 
 */
package net.sf.jabb.util.retry;

/**
 * Exception thrown in the situation that interruption happened while waiting or preparing for next attempt.
 * The cause field contains the cause of the interruption which normally is an instance of InterruptedException.
 * The exception happened during last attempt if exist, will be added as one of the suppressed exceptions.
 * @author James Hu
 *
 */
public class InterruptedBeforeAttemptException extends AttemptStrategyException {
	private static final long serialVersionUID = 6766685242106896432L;

	public InterruptedBeforeAttemptException(InterruptedException e, Attempt<?> lastAttempt){
		super("Interrupted before next attempt: " + (lastAttempt.getTotalAttempts() + 1), e);
		this.lastAttempt = lastAttempt;
		if (lastAttempt.hasException()){
			this.addSuppressed(lastAttempt.getException());
		}
	}
}
