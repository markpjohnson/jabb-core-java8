/**
 * 
 */
package net.sf.jabb.util.retry;

import java.time.Instant;
import java.util.Optional;

/**
 * Information about an attempt of an operation.
 * 
 * @author James Hu
 *
 * @param <R>	the return type of the attempted operation
 */
public class Attempt<R> {
    private int totalAttempts;
    private Instant firstAttemptStartTime;
    private Instant lastAttemptFinishTime;
    private Object context;
    
    private R result;
    private Optional<Exception> exception;
    
    private Attempt(){
    }
    
    /**
     * Construct an instance representing an attempt with a result.
     * @param <T> type of the result type
     * @param context					the context object
     * @param totalAttempts				total number of attempts
     * @param firstAttemptStartTime		start time of the first attempt
     * @param lastAttemptFinishTime		finish time of the last attempt
     * @param result					result from the attempt
     * @return	a new instance representing the attempt
     */
    static public <T> Attempt<T> withResult(Object context, int totalAttempts, Instant firstAttemptStartTime, Instant lastAttemptFinishTime, T result){
    	Attempt<T> instance = new Attempt<T>();
    	
    	instance.context = context;
    	instance.totalAttempts = totalAttempts;
    	instance.firstAttemptStartTime = firstAttemptStartTime;
    	instance.lastAttemptFinishTime = lastAttemptFinishTime;
    	instance.result = result;
    	instance.exception = Optional.empty();
    	
    	return instance;
    }
    
    /**
     * Construct an instance representing a failed attempt caused by a Throwable
     * @param context					the context object
     * @param totalAttempts				total number of attempts
     * @param firstAttemptStartTime		start time of the first attempt
     * @param lastAttemptFinishTime		finish time of the last attempt
     * @param exception					the Throwable happened during the attempt
     * @return	a new instance representing the attempt
     */
    static public Attempt<Void> withException(Object context, int totalAttempts, Instant firstAttemptStartTime, Instant lastAttemptFinishTime, Exception exception){
    	Attempt<Void> instance = new Attempt<Void>();
    	
    	instance.context = context;
    	instance.totalAttempts = totalAttempts;
    	instance.firstAttemptStartTime = firstAttemptStartTime;
    	instance.lastAttemptFinishTime = lastAttemptFinishTime;
    	instance.result = null;
    	instance.exception = Optional.ofNullable(exception);
    	
    	return instance;
    }
    
    /**
     * Check whether the attempt has got a result. An attempt cannot have both a result and an exception.
     * @return	true if there is a result, false otherwise
     */
    public boolean hasResult(){
    	return !exception.isPresent();
    }
    
    /**
     * Check whether exception happened during the attempt. An attempt cannot have both a result and an exception.
     * @return true if there is an exception, false otherwise
     */
    public boolean hasException(){
    	return exception.isPresent();
    }
    
	public int getTotalAttempts() {
		return totalAttempts;
	}
	public void setTotalAttempts(int totalAttempts) {
		this.totalAttempts = totalAttempts;
	}
	/**
	 * Get the time the first attempt started
	 * @return	the time that the first attempt started at
	 */
	public Instant getFirstAttemptStartTime() {
		return firstAttemptStartTime;
	}
	public void setFirstAttemptStartTime(Instant firstAttemptStartTime) {
		this.firstAttemptStartTime = firstAttemptStartTime;
	}
	public R getResult() {
		return result;
	}
	public void setResult(R result) {
		this.result = result;
	}
	public Exception getException() {
		return exception.get();
	}
	public void setException(Exception exception) {
		this.exception = Optional.ofNullable(exception);
	}
	/**
	 * Get the time the last attempt finished
	 * @return	the time that the last attempt finished at
	 */
	public Instant getLastAttemptFinishTime() {
		return lastAttemptFinishTime;
	}
	public void setLastAttemptFinishTime(Instant lastAttemptFinishTime) {
		this.lastAttemptFinishTime = lastAttemptFinishTime;
	}

	public Object getContext() {
		return context;
	}

	public void setContext(Object context) {
		this.context = context;
	}

}
