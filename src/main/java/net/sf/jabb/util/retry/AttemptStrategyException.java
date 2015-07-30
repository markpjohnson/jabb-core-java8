package net.sf.jabb.util.retry;

public class AttemptStrategyException extends Exception {

	protected Attempt<?> lastAttempt;

	public AttemptStrategyException() {
		super();
	}

	public AttemptStrategyException(String message) {
		super(message);
	}

	public AttemptStrategyException(Throwable cause) {
		super(cause);
	}

	public AttemptStrategyException(String message, Throwable cause) {
		super(message, cause);
	}

	public AttemptStrategyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public Attempt<?> getLastAttempt() {
		return lastAttempt;
	}

}