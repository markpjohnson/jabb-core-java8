/**
 * 
 */
package net.sf.jabb.util.retry;

import java.time.Duration;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.TimeLimiter;

import net.sf.jabb.util.ex.ExceptionUncheckUtility;

/**
 * The strategy controlling how an operation (Runnable or Callable) will be attempted multiple times.
 * @author James Hu
 *
 */
public class AttemptStrategy extends AttemptStrategyImpl {
	
	/**
	 * Attempt the Runnable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param runnable			the Runnable to be attempted
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
	 */
    public void runThrowingAll(Runnable runnable)
    		throws TooManyAttemptsException, InterruptedBeforeAttemptException, Exception {
    	callThrowingAll(() -> {runnable.run(); return null;}, null);
    }
    
    /**
     * Attempt the Runnable according to the strategies defined, throwing all the exceptions in their original forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
	 * @param runnable			the Runnable to be attempted
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
     */
    public void runThrowingUncheckedAll(Runnable runnable){
    	ExceptionUncheckUtility.uncheck(()->callThrowingAll(() -> {runnable.run(); return null;}, null));
    }

    /**
     * Attempt the Runnable according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param runnable			the Runnable to be attempted
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public void runThrowingOriginal(Runnable runnable) throws Exception{
    	try {
    		callThrowingAll(() -> {runnable.run(); return null;}, null);
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
     * Attempt the Runnable according to the strategies defined, throwing exceptions in their original or modified forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param runnable			the Runnable to be attempted
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
     */
    public void run(Runnable runnable){
       	ExceptionUncheckUtility.uncheck(()->runThrowingOriginal(runnable));
    }
    


    /**
     * Constructor initiating an empty strategy ready for further configuring.
     */
    public AttemptStrategy(){
    	super();
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
     * Sets the backoff strategy used to decide how long to backoff before next attempt. The default strategy is to not backoff at all .
     *
     * @param backoffStrategy the strategy used to decide how long to backoff before next attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a backoff strategy has already been set.
     */
    public AttemptStrategy withBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) throws IllegalStateException {
        return (AttemptStrategy) super.withBackoffStrategy(backoffStrategy);
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
     * Sets the time limit for each of the attempts. By default there will be no time limit applied.
     *
     * @param attemptTimeLimiter the TimeLimiter implementation
     * @param attemptTimeLimit the maximum time allowed for an attempt
     * @return <code>this</code>
     * @throws IllegalStateException if a time limit has already been set.
     */
    public AttemptStrategy withAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        return (AttemptStrategy) super.withAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
    }

    /**
     * Add a listener. More than one listeners can be added.
     * @param listener a listener
     */
    public AttemptStrategy withAttemptListener(@Nonnull AttemptListener listener) {
        return (AttemptStrategy) super.withAttemptListener(listener);
    }
    
    ////////////////////////
    
    public AttemptStrategy retryIfAttemptHasException(@Nonnull Predicate<Attempt<Void>> exceptionPredicate) {
        return (AttemptStrategy) super.retryIfAttemptHasException(exceptionPredicate);
    }

    public AttemptStrategy retryIfException(@Nonnull Predicate<Exception> exceptionPredicate) {
        return (AttemptStrategy) super.retryIfException(exceptionPredicate);
    }

    public AttemptStrategy retryIfException(@Nonnull Class<? extends Exception> exceptionClass) {
        return (AttemptStrategy) super.retryIfException(exceptionClass);
    }

    public AttemptStrategy retryIfRuntimeException() {
        return (AttemptStrategy) super.retryIfRuntimeException();
    }

    public AttemptStrategy retryIfException() {
        return (AttemptStrategy) super.retryIfException();
    }

    
    ///////////  switching to CallableAttemptStrategy<R>
    public <R> CallableAttemptStrategy<R> retryIfAttemptHasResult(@Nonnull Predicate<Attempt<R>> resultPredicate) {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfAttemptHasResult(resultPredicate);
    }

    public <R> CallableAttemptStrategy<R> retryIfResult(@Nonnull Predicate<R> resultPredicate) {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfResult(resultPredicate);
    }

    public <R> CallableAttemptStrategy<R> retryIfResultIsNull() {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfResultIsNull();
    }

    public <R> CallableAttemptStrategy<R>  toCallableAttemptStrategy(){
    	return new CallableAttemptStrategy<R>(this);
    }
    


}
