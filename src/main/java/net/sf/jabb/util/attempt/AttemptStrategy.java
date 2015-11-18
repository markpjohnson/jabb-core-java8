/**
 * 
 */
package net.sf.jabb.util.attempt;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.TimeLimiter;

import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.RunnableThrowsExceptions;
import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.WaitStrategy;

/**
 * The strategy controlling how an operation (Runnable or Callable) will be attempted multiple times.
 * @author James Hu
 *
 */
public class AttemptStrategy extends AttemptStrategyImpl {
	
	/**
	 * Attempt the RunnableThrowsExceptions according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param runnable			the RunnableThrowsExceptions to be attempted
	 * @throws AttemptException		Any exception happened while applying the attempt strategy.
	 * 								For example, {@link TooManyAttemptsException} if no more attempt is allowed by the stop strategy,
	 * 								or {@link InterruptedBeforeAttemptException} if InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
	 */
    public void runThrowingAll(RunnableThrowsExceptions runnable)
    		throws AttemptException, Exception {
    	callThrowingAll(() -> {runnable.run(); return null;}, null);
    }
    
	/**
	 * Attempt the Runnable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param runnable			the Runnable to be attempted
	 * @throws AttemptException		Any exception happened while applying the attempt strategy.
	 * 								For example, {@link TooManyAttemptsException} if no more attempt is allowed by the stop strategy,
	 * 								or {@link InterruptedBeforeAttemptException} if InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 */
    public void runThrowingAttempt(Runnable runnable)
    		throws AttemptException {
    	ExceptionUncheckUtility.runThrowingUnchecked(()->callThrowingAll(() -> {runnable.run(); return null;}, null));
    }
    
    /**
     * Attempt the RunnableThrowsExceptions according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param runnable			the RunnableThrowsExceptions to be attempted
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public void runThrowingSuppressed(RunnableThrowsExceptions runnable) throws Exception{
    	try {
    		callThrowingAll(() -> {runnable.run(); return null;}, null);
		} catch (TooManyAttemptsException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(new TooManyAttemptsException(e.getMessage()));	// avoid loop of suppressed exceptions
				throw lastCause;
			}else{	// should never happen
				throw e;
			}
		} catch (InterruptedBeforeAttemptException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(new InterruptedBeforeAttemptException(e.getMessage()));	// avoid loop of suppressed exceptions
				throw lastCause;
			}else{	// should never happen
				throw e;
			}
		}
    }
    
  
    /**
     * Synonym of {@link #runThrowingSuppressed(net.sf.jabb.util.ex.ExceptionUncheckUtility.RunnableThrowsExceptions)}
	 * @param runnable			the RunnableThrowsExceptions to be attempted
 	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
    */
    public void run(RunnableThrowsExceptions runnable) throws Exception{
       	runThrowingSuppressed(runnable);
    }
    
    /**
     * Constructor initiating an empty strategy
     */
    public AttemptStrategy() {
		super();
	}

    /**
     * Constructor initiating an strategy by copying configurations from another instance.
     * @param that		another attempt strategy
     */
    public AttemptStrategy(AttemptStrategyImpl that){
    	super(that);
    }
    
    /**
     * Create an instance with empty strategy
     * @return	the new instance
     */
    static public AttemptStrategy create(){
    	return new AttemptStrategy();
    }

    /**
     * Create an instance with configurations copied from another instance
     * @param <R> type of the result of the attempt
     * @param that	another instance
     * @return	the new instance
     */
    static public <R> AttemptStrategyWithRetryOnResult<R> create(AttemptStrategyWithRetryOnResult<R> that){
   		return new AttemptStrategyWithRetryOnResult<R>(that);
    }

    /**
     * Create an instance with configurations copied from another instance
     * @param that	another instance
     * @return	the new instance
     */
    static public AttemptStrategy create(AttemptStrategyImpl that){
    	return new AttemptStrategy(that);
    }

	///////////////////////////

    /**
     * Sets the stop strategy used to decide when to stop attempting. The default strategy is to not stop at all .
     *
     * @param stopStrategy the strategy used to decide when to stop retrying
     * @return <code>this</code>
     * @throws IllegalStateException if a stop strategy has already been set.
     */
    public AttemptStrategy withStopStrategy(@Nonnull StopStrategy stopStrategy) throws IllegalStateException {
        return (AttemptStrategy) super.withStopStrategy(stopStrategy);
    }

    /**
     * Sets the stop strategy used to decide when to stop attempting. The default strategy is to not stop at all .
     *
     * @param stopStrategy the strategy used to decide when to stop retrying
     * @return <code>this</code>
     */
    public AttemptStrategy overrideStopStrategy(@Nonnull StopStrategy stopStrategy) {
        return (AttemptStrategy) super.overrideStopStrategy(stopStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
    public AttemptStrategy withBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy) throws IllegalStateException {
        return (AttemptStrategy) super.withBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     */
    public AttemptStrategy overrideBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy) {
        return (AttemptStrategy) super.overrideBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
	public AttemptStrategy withBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) throws IllegalStateException {
    	return (AttemptStrategy) super.withBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     */
	public AttemptStrategy overrideBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) {
    	return (AttemptStrategy) super.overrideBackoffStrategy(backoffStrategy);
    }

	/**
     * Sets the wait strategy used to decide how to perform waiting before next attempt. The default strategy is to use Thread.sleep() .
     *
     * @param waitStrategy the strategy used to decide how to perform waiting before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a wait strategy has already been set.
     */
    public AttemptStrategy withWaitStrategy(@Nonnull WaitStrategy waitStrategy) throws IllegalStateException {
    	return (AttemptStrategy) super.withWaitStrategy(waitStrategy);
    }
    
	/**
     * Sets the wait strategy used to decide how to perform waiting before next attempt. The default strategy is to use Thread.sleep() .
     *
     * @param waitStrategy the strategy used to decide how to perform waiting before next attempt
     * @return <code>this</code>
     */
    public AttemptStrategy overrideWaitStrategy(@Nonnull WaitStrategy waitStrategy) {
    	return (AttemptStrategy) super.overrideWaitStrategy(waitStrategy);
    }
    
    /**
     * Sets the time limit for each of the attempts. By default there will be no time limit applied.
     *
     * @param attemptTimeLimiter the TimeLimiter implementation
     * @param attemptTimeLimit the maximum time allowed for an attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a time limit has already been set.
     */
    public AttemptStrategy withAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit) throws IllegalStateException{
        return (AttemptStrategy) super.withAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Sets the time limit for each of the attempts. By default there will be no time limit applied.
     *
     * @param attemptTimeLimiter the TimeLimiter implementation
     * @param attemptTimeLimit the maximum time allowed for an attempt
     * @return <code>this</code>
     */
    public AttemptStrategy overrideAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        return (AttemptStrategy) super.overrideAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Add a listener. More than one listeners can be added and they will be called in the same
     * order they are added.
     * @param listener a listener
     * @return <code>this</code>
     */
    public AttemptStrategy withAttemptListener(@Nonnull AttemptListener listener) {
        return (AttemptStrategy) super.withAttemptListener(listener);
    }
    
    ////////////////////////
    
    /**
     * Adds a predicate to decide whether next attempt is needed when exception happened in previous attempt.
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @param exceptionPredicate The predicate to be called when exception happened in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    public AttemptStrategy retryIfAttemptHasException(@Nonnull Predicate<Attempt<Void>> exceptionPredicate) {
        return (AttemptStrategy) super.retryIfAttemptHasException(exceptionPredicate);
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when exception happened in previous attempt.
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @param exceptionPredicate The predicate to be called when exception happened in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    public AttemptStrategy retryIfException(@Nonnull Predicate<Exception> exceptionPredicate) {
        return (AttemptStrategy) super.retryIfException(exceptionPredicate);
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when exception of specified type happened in previous attempt.
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @param exceptionClass		type of the exception that the predicate applies to
     * @param exceptionPredicate The predicate to be called when exception of specified type happened in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
	public <E extends Exception> AttemptStrategy retryIfException(@Nonnull Class<E> exceptionClass, @Nonnull Predicate<E> exceptionPredicate) {
		return (AttemptStrategy) super.retryIfException(exceptionClass, exceptionPredicate);
	}
	
    /**
     * Add a predicate that if a specific type of exception happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @param exceptionClass  type of the exception that will cause next attempt. 
     * 						exceptionClass.isAssignableFrom(...) will be used to decide whether an exception is of this type.
     * @return <code>this</code>
     */
    public AttemptStrategy retryIfException(@Nonnull Class<? extends Exception> exceptionClass) {
        return (AttemptStrategy) super.retryIfException(exceptionClass);
    }

    /**
     * Add a predicate that if any RuntimeException happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @return <code>this</code>
     */
    public AttemptStrategy retryIfRuntimeException() {
        return (AttemptStrategy) super.retryIfRuntimeException();
    }

    /**
     * Add a predicate that if any exception happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propagated and there will be no further attempt.
     * @return <code>this</code>
     */
    public AttemptStrategy retryIfException() {
        return (AttemptStrategy) super.retryIfException();
    }

    
    ///////////  switching to AttemptStrategyWithRetryOnResult<R>
    
    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param <R> type of the result of the attempt
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return a new instance of AttemptStrategyWithRetryOnResult with the same configuration as this
     */
    public <R> AttemptStrategyWithRetryOnResult<R> retryIfAttemptHasResult(@Nonnull Predicate<Attempt<R>> resultPredicate) {
    	AttemptStrategyWithRetryOnResult<R> that = new AttemptStrategyWithRetryOnResult<R>(this);
    	return that.retryIfAttemptHasResult(resultPredicate);
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param <R> type of the result of the attempt
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return a new instance of AttemptStrategyWithRetryOnResult with the same configuration as this
     */
    public <R> AttemptStrategyWithRetryOnResult<R> retryIfResult(@Nonnull Predicate<R> resultPredicate) {
    	AttemptStrategyWithRetryOnResult<R> that = new AttemptStrategyWithRetryOnResult<R>(this);
    	return that.retryIfResult(resultPredicate);
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when a specified value equals to the result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param <R> type of the result of the attempt
     * @param resultValue	The value to be compared when a result was returned in previous attempt.
     * 					resultValue.equals(...) will be used.
     * @return <code>this</code>
     */
    public <R> AttemptStrategyWithRetryOnResult<R> retryIfResultEquals(@Nonnull R resultValue) {
    	AttemptStrategyWithRetryOnResult<R> that = new AttemptStrategyWithRetryOnResult<R>(this);
        return that.retryIfResultEquals(resultValue);
    }

    /**
     * Adds a predicate to make sure that if there is a null result from an attempt there will be a need for next attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param <R> type of the result of the attempt
     * @return a new instance of AttemptStrategyWithRetryOnResult with the same configuration as this
     */
     public <R> AttemptStrategyWithRetryOnResult<R> retryIfResultIsNull() {
    	AttemptStrategyWithRetryOnResult<R> that = new AttemptStrategyWithRetryOnResult<R>(this);
    	return that.retryIfResultIsNull();
    }

	/**
	 * Attempt the Callable according to the strategies defined, throwing all the exceptions in their original forms.
     * @param <R> type of the result of the attempt
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws AttemptException		Any exception happened while applying the attempt strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
	 */
    public <R> R callThrowingAll(Callable<R> callable)
    		throws AttemptException, Exception {
    	return callThrowingAll(callable, null);
    }
    
    /**
     * Attempt the Callable according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
     * @param <R> type of the result of the attempt
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public <R> R callThrowingSuppressed(Callable<R> callable) throws Exception{
    	try {
    		return callThrowingAll(callable, null);
		} catch (TooManyAttemptsException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(new TooManyAttemptsException(e.getMessage()));
				throw lastCause;
			}else{	// should never happen
				throw e;
			}
		} catch (InterruptedBeforeAttemptException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(new InterruptedBeforeAttemptException(e.getMessage()));
				throw lastCause;
			}else{	// should never happen
				throw e;
			}
		}
    }
    
    /**
     * Synonym of {@link #callThrowingSuppressed(Callable)}
     * @param <R> type of the result of the attempt
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public <R> R call(Callable<R> callable) throws Exception{
       	return callThrowingSuppressed(callable);
    }
 

}
