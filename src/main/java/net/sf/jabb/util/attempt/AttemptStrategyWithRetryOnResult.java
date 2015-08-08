/**
 * 
 */
package net.sf.jabb.util.attempt;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.WaitStrategy;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.TimeLimiter;

/**
 * The strategy controlling how a Callable will be attempted multiple times.
 * @author James Hu
 *
 * @param <R>	Return value type of the Callable
 */
public class AttemptStrategyWithRetryOnResult<R> extends AttemptStrategyImpl {
	protected Predicate<Attempt<R>> retryConditionOnResult;
	
	/**
	 * Attempt the Callable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws AttemptException		Any exception happened while applying the attempt strategy.
	 * 								For example, {@link TooManyAttemptsException} if no more attempt is allowed by the stop strategy,
	 * 								or {@link InterruptedBeforeAttemptException} if InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
	 */
    public R callThrowingAll(Callable<R> callable)
    		throws AttemptException, Exception {
    	return callThrowingAll(callable, retryConditionOnResult);
    }
    
    /**
     * Synonym of {@link #callThrowingAll(Callable)}
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws AttemptException		Any exception happened while applying the attempt strategy.
	 * 								For example, {@link TooManyAttemptsException} if no more attempt is allowed by the stop strategy,
	 * 								or {@link InterruptedBeforeAttemptException} if InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
     */
    public R call(Callable<R> callable) throws AttemptException, Exception {
    	return callThrowingAll(callable, retryConditionOnResult);
    }
    


    /**
     * Constructor initiating an strategy by copying configurations from another instance.
     * @param that		another attempt strategy
     */
    @SuppressWarnings("unchecked")
	AttemptStrategyWithRetryOnResult(AttemptStrategyImpl that){
    	super(that);
    	if (that instanceof AttemptStrategyWithRetryOnResult){
    		this.retryConditionOnResult = ((AttemptStrategyWithRetryOnResult<R>)that).retryConditionOnResult;
    	}
    }
    
    /////////////////////////////////
    
    /**
     * Sets the stop strategy used to decide when to stop attempting. The default strategy is to not stop at all .
     *
     * @param stopStrategy the strategy used to decide when to stop retrying
     * @return <code>this</code>
     * @throws IllegalStateException if a stop strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withStopStrategy(@Nonnull StopStrategy stopStrategy) throws IllegalStateException {
        return (AttemptStrategyWithRetryOnResult<R>) super.withStopStrategy(stopStrategy);
    }

    /**
     * Sets the stop strategy used to decide when to stop attempting. The default strategy is to not stop at all .
     *
     * @param stopStrategy the strategy used to decide when to stop retrying
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> overrideStopStrategy(@Nonnull StopStrategy stopStrategy){
        return (AttemptStrategyWithRetryOnResult<R>) super.overrideStopStrategy(stopStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy) throws IllegalStateException {
        return (AttemptStrategyWithRetryOnResult<R>) super.withBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> overrideBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy){
        return (AttemptStrategyWithRetryOnResult<R>) super.overrideBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) throws IllegalStateException {
    	return (AttemptStrategyWithRetryOnResult<R>) super.withBackoffStrategy(backoffStrategy);
    }
    
    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> overrideBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy){
    	return (AttemptStrategyWithRetryOnResult<R>) super.overrideBackoffStrategy(backoffStrategy);
    }
    
    /**
     * Sets the wait strategy used to decide how to perform waiting before next attempt. The default strategy is to use Thread.sleep() .
     *
     * @param waitStrategy the strategy used to decide how to perform waiting before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a wait strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withWaitStrategy(@Nonnull WaitStrategy waitStrategy) throws IllegalStateException {
    	return (AttemptStrategyWithRetryOnResult<R>) super.withWaitStrategy(waitStrategy);
    }
    
    /**
     * Sets the wait strategy used to decide how to perform waiting before next attempt. The default strategy is to use Thread.sleep() .
     *
     * @param waitStrategy the strategy used to decide how to perform waiting before next attempt
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> overrideWaitStrategy(@Nonnull WaitStrategy waitStrategy) {
    	return (AttemptStrategyWithRetryOnResult<R>) super.overrideWaitStrategy(waitStrategy);
    }
    
    /**
     * Sets the time limit for each of the attempts. By default there will be no time limit applied.
     *
     * @param attemptTimeLimiter the TimeLimiter implementation
     * @param attemptTimeLimit the maximum time allowed for an attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a time limit has already been set.
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit) throws IllegalStateException{
        return (AttemptStrategyWithRetryOnResult<R>) super.withAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Sets the time limit for each of the attempts. By default there will be no time limit applied.
     *
     * @param attemptTimeLimiter the TimeLimiter implementation
     * @param attemptTimeLimit the maximum time allowed for an attempt
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> overrideAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        return (AttemptStrategyWithRetryOnResult<R>) super.overrideAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Add a listener. More than one listeners can be added.
     * @param listener a listener
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> withAttemptListener(@Nonnull AttemptListener listener) {
        return (AttemptStrategyWithRetryOnResult<R>) super.withAttemptListener(listener);
    }
    
    
    /////////////////////////////////
    
    /**
     * Adds a predicate to decide whether next attempt is needed when exception happened in previous attempt.
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propageted and there will be no further attempt.
     * @param exceptionPredicate The predicate to be called when exception happened in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> retryIfAttemptHasException(@Nonnull Predicate<Attempt<Void>> exceptionPredicate) {
    	return (AttemptStrategyWithRetryOnResult<R>) super.retryIfAttemptHasException(exceptionPredicate);
    }
    
    /**
     * Adds a predicate to decide whether next attempt is needed when exception happened in previous attempt.
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propageted and there will be no further attempt.
     * @param exceptionPredicate The predicate to be called when exception happened in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> retryIfException(@Nonnull Predicate<Exception> exceptionPredicate) {
        return (AttemptStrategyWithRetryOnResult<R>) super.retryIfException(exceptionPredicate);
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
	@SuppressWarnings("unchecked")
	public <E extends Exception> AttemptStrategyWithRetryOnResult<R> retryIfException(@Nonnull Class<E> exceptionClass, @Nonnull Predicate<E> exceptionPredicate) {
		return (AttemptStrategyWithRetryOnResult<R>) super.retryIfException(exceptionClass, exceptionPredicate);
	}
	
    /**
     * Add a predicate that if a specific type of exception happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propageted and there will be no further attempt.
     * @param exceptionClass  type of the exception that will cause next attempt. 
     * 						exceptionClass.isAssignableFrom(...) will be used to decide whether an exception is of this type.
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> retryIfException(@Nonnull Class<? extends Exception> exceptionClass) {
        return (AttemptStrategyWithRetryOnResult<R>) super.retryIfException(exceptionClass);
    }

    /**
     * Add a predicate that if any RuntimeException happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propageted and there will be no further attempt.
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> retryIfRuntimeException() {
        return (AttemptStrategyWithRetryOnResult<R>) super.retryIfRuntimeException();
    }

    /**
     * Add a predicate that if any exception happened during an attempt, there needs to be a next attempt. 
     * retryIf*Exceptin(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Exceptin(...) has been called or if an exception happened during an attempt cannot
     * make any of the predicates true, it will be propageted and there will be no further attempt.
     * @return <code>this</code>
     */
    @SuppressWarnings("unchecked")
	public AttemptStrategyWithRetryOnResult<R> retryIfException() {
        return (AttemptStrategyWithRetryOnResult<R>) super.retryIfException();
    }


    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    public AttemptStrategyWithRetryOnResult<R> retryIfAttemptHasResult(@Nonnull Predicate<Attempt<R>> resultPredicate) {
        Preconditions.checkNotNull(resultPredicate, "resultPredicate may not be null");
        retryConditionOnResult = retryConditionOnResult == null ? resultPredicate
        		: retryConditionOnResult.or(resultPredicate);
        return this;
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return <code>this</code>
     */
    public AttemptStrategyWithRetryOnResult<R> retryIfResult(@Nonnull Predicate<R> resultPredicate) {
        Preconditions.checkNotNull(resultPredicate, "resultPredicate may not be null");
        return retryIfAttemptHasResult(attempt->resultPredicate.test(attempt.getResult()));
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when a specified value equals to the result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultValue	The value to be compared when a result was returned in previous attempt.
     * 					resultValue.equals(...) will be used.
     * @return <code>this</code>
     */
    public AttemptStrategyWithRetryOnResult<R> retryIfResultEquals(@Nonnull R resultValue) {
        Preconditions.checkNotNull(resultValue, "resultValue may not be null");
        return retryIfAttemptHasResult(attempt->resultValue.equals(attempt.getResult()));
    }

    /**
     * Adds a predicate to make sure that if there is a null result from an attempt there will be a need for next attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @return <code>this</code>
     */
    public AttemptStrategyWithRetryOnResult<R> retryIfResultIsNull() {
        return retryIfAttemptHasResult(attempt-> null == attempt.getResult());
    }



}
