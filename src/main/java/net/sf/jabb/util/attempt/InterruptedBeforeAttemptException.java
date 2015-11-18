/**
 * 
 */
package net.sf.jabb.util.attempt;

/**
 * Exception thrown in the situation that interruption happened while waiting or preparing for next attempt.
 * The cause field contains the cause of the interruption which normally is an instance of InterruptedException.
 * The exception happened during last attempt if exist, will be added as one of the suppressed exceptions.
 * @author James Hu
 *
 */
public class InterruptedBeforeAttemptException extends AttemptException {
	private static final long serialVersionUID = 6766685242106896432L;

	public InterruptedBeforeAttemptException(InterruptedException e, Attempt<?> lastAttempt){
		super("Interrupted before next attempt: " + (lastAttempt.getTotalAttempts() + 1), e);
		this.lastAttempt = lastAttempt;
		if (lastAttempt.hasException()){
			this.addSuppressed(lastAttempt.getException());
		}
	}
	
	public InterruptedBeforeAttemptException(String message){
		super(message);
	}
	
	public InterruptedBeforeAttemptException(String message, Throwable cause){
		super(message, cause);
	}
	
	/**
	 * Create a copy of this but without the suppressed exceptions
	 * @return	the copy without suppressed exceptions (but with the cause exception)
	 */
	public InterruptedBeforeAttemptException copyWithoutSuppressed(){
		InterruptedBeforeAttemptException copy = new InterruptedBeforeAttemptException(this.getMessage(), this.getCause());
		copy.setStackTrace(this.getStackTrace());
		return copy;
	}

}
