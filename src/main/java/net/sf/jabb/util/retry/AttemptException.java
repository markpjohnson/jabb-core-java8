package net.sf.jabb.util.retry;

/**
 * Exception happened while applying the {@link AttemptStrategy}
 * 
 * @author James Hu
 *
 */
public class AttemptException extends Exception {
	private static final long serialVersionUID = 6260013304441217136L;
	
	protected Attempt<?> lastAttempt;

	public AttemptException() {
		super();
	}

	public AttemptException(String message) {
		super(message);
	}

	public AttemptException(Throwable cause) {
		super(cause);
	}

	public AttemptException(String message, Throwable cause) {
		super(message, cause);
	}

	public AttemptException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public Attempt<?> getLastAttempt() {
		return lastAttempt;
	}

}