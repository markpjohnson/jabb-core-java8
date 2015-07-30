/**
 * 
 */
package net.sf.jabb.util.retry;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import net.sf.jabb.util.ex.ExceptionUncheckUtility;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.TimeLimiter;

/**
 * The strategy controlling how an operation (Runnable or Callable) will be attempted multiple times.
 * @author James Hu
 *
 * @param <R>	Return value type of the Callable
 */
public class CallableAttemptStrategy<R> extends AttemptStrategyImpl {
	protected Predicate<Attempt<R>> retryConditionOnResult;
	
	/**
	 * Attempt the Callable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
	 */
    public R callThrowingAll(Callable<R> callable)
    		throws TooManyAttemptsException, InterruptedBeforeAttemptException, Exception {
    	return callThrowingAll(callable, retryConditionOnResult);
    }
    
    /**
	 * Attempt the Callable according to the strategies defined, throwing all the exceptions in their original forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
     */
    public R callThrowingUncheckedAll(Callable<R> callable){
    	return ExceptionUncheckUtility.uncheck(()->callThrowingAll(callable, retryConditionOnResult));
    }

    /**
     * Attempt the Callable according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public R callThrowingOriginal(Callable<R> callable) throws Exception{
    	try {
    		return callThrowingAll(callable, retryConditionOnResult);
		} catch (TooManyAttemptsException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			lastCause.addSuppressed(e);
			throw lastCause;
		} catch (InterruptedBeforeAttemptException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			lastCause.addSuppressed(e.getCause());
			throw lastCause;
		}
    }
    
    /**
     * Attempt the Callable according to the strategies defined, throwing exceptions in their original or modified forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public R call(Callable<R> callable){
       	return ExceptionUncheckUtility.uncheck(()->callThrowingOriginal(callable));
    }
    


    /**
     * Constructor initiating an empty strategy ready for further configuring.
     */
    public CallableAttemptStrategy(){
    	super();
    }

    /**
     * Constructor initiating an strategy by copying configurations from another instance.
     * @param that
     */
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy(AttemptStrategyImpl that){
    	super(that);
    	if (that instanceof CallableAttemptStrategy){
    		this.retryConditionOnResult = ((CallableAttemptStrategy<R>)that).retryConditionOnResult;
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
	public CallableAttemptStrategy<R> withStopStrategy(@Nonnull StopStrategy stopStrategy) throws IllegalStateException {
        return (CallableAttemptStrategy<R>) super.withStopStrategy(stopStrategy);
    }

    /**
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> withBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) throws IllegalStateException {
        return (CallableAttemptStrategy<R>) super.withBackoffStrategy(backoffStrategy);
    }

    /**
     * Sets the wait strategy used to decide how to perform waiting before next attempt. The default strategy is to use Thread.sleep() .
     *
     * @param waitStrategy the strategy used to decide how to perform waiting before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a wait strategy has already been set.
     */
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> withWaitStrategy(@Nonnull WaitStrategy waitStrategy) throws IllegalStateException {
    	return (CallableAttemptStrategy<R>) super.withWaitStrategy(waitStrategy);
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
	public CallableAttemptStrategy<R> withAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        return (CallableAttemptStrategy<R>) super.withAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Add a listener. More than one listeners can be added.
     * @param listener a listener
     */
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> withAttemptListener(@Nonnull AttemptListener listener) {
        return (CallableAttemptStrategy<R>) super.withAttemptListener(listener);
    }
    
    
    /////////////////////////////////
    
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> retryIfAttemptHasException(@Nonnull Predicate<Attempt<Void>> exceptionPredicate) {
    	return (CallableAttemptStrategy<R>) super.retryIfAttemptHasException(exceptionPredicate);
    }
    
    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> retryIfException(@Nonnull Predicate<Exception> exceptionPredicate) {
        return (CallableAttemptStrategy<R>) super.retryIfException(exceptionPredicate);
    }

    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> retryIfException(@Nonnull Class<? extends Exception> exceptionClass) {
        return (CallableAttemptStrategy<R>) super.retryIfException(exceptionClass);
    }

    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> retryIfRuntimeException() {
        return (CallableAttemptStrategy<R>) super.retryIfRuntimeException();
    }

    @SuppressWarnings("unchecked")
	public CallableAttemptStrategy<R> retryIfException() {
        return (CallableAttemptStrategy<R>) super.retryIfException();
    }


    public CallableAttemptStrategy<R> retryIfAttemptHasResult(@Nonnull Predicate<Attempt<R>> resultPredicate) {
        Preconditions.checkNotNull(resultPredicate, "resultPredicate may not be null");
        retryConditionOnResult = retryConditionOnResult == null ? resultPredicate
        		: retryConditionOnResult.or(resultPredicate);
        return this;
    }

    public CallableAttemptStrategy<R> retryIfResult(@Nonnull Predicate<R> resultPredicate) {
        Preconditions.checkNotNull(resultPredicate, "resultPredicate may not be null");
        return retryIfAttemptHasResult(attempt->resultPredicate.test(attempt.getResult()));
    }

    public CallableAttemptStrategy<R> retryIfResultIsNull() {
        return retryIfAttemptHasResult(attempt-> null == attempt.getResult());
    }



}
