/**
 * 
 */
package net.sf.jabb.util.retry;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.ex.ExceptionUncheckUtility.RunnableWithExceptions;
import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.WaitStrategy;

import com.google.common.util.concurrent.TimeLimiter;

/**
 * The strategy controlling how an operation (Runnable or Callable) will be attempted multiple times.
 * @author James Hu
 *
 */
public class AttemptStrategy extends AttemptStrategyImpl {
	
	/**
	 * Attempt the RunnableWithExceptions according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param runnable			the RunnableWithExceptions to be attempted
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
	 */
    public void runThrowingAll(RunnableWithExceptions runnable)
    		throws TooManyAttemptsException, InterruptedBeforeAttemptException, Exception {
    	callThrowingAll(() -> {runnable.accept(); return null;}, null);
    }
    
	/**
	 * Attempt the Runnable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param runnable			the Runnable to be attempted
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
	 */
    public void runThrowingAttemptStrategyException(Runnable runnable)
    		throws TooManyAttemptsException, InterruptedBeforeAttemptException {
    	ExceptionUncheckUtility.uncheck(()->callThrowingAll(() -> {runnable.run(); return null;}, null));
    }
    
    /**
     * Attempt the RunnableWithExceptions according to the strategies defined, throwing all the exceptions in their original forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
	 * @param runnable			the RunnableWithExceptions to be attempted
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies
     */
    public void runThrowingUncheckedAll(RunnableWithExceptions runnable){
    	ExceptionUncheckUtility.uncheck(()->runThrowingAll(runnable));
    }

    /**
     * Attempt the RunnableWithExceptions according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param runnable			the RunnableWithExceptions to be attempted
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws TooManyAttemptsException  If there is no exception happened during last attempt but there is a TooManyAttemptsException
	 * @throws InterruptedException  If there is no exception happened during last attempt but there is a InterruptedException
     */
    public void runThrowingSuppressed(RunnableWithExceptions runnable) throws Exception, TooManyAttemptsException, InterruptedException{
    	try {
    		callThrowingAll(() -> {runnable.accept(); return null;}, null);
		} catch (TooManyAttemptsException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(e);
				throw lastCause;
			}else{
				throw e;
			}
		} catch (InterruptedBeforeAttemptException e) {
			Attempt<?> lastAttempt = e.getLastAttempt();
			Exception lastCause = lastAttempt.getException();
			if (lastCause != null){
				lastCause.addSuppressed(e);
				throw lastCause;
			}else{
				throw e;
			}
		}
    }
    
    /**
     * Attempt the RunnableWithExceptions according to the strategies defined, throwing exceptions in their original or modified forms.
     * Possible exceptions thrown from this method are not declared in the method signature. But they will still be thrown and need to be handled.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param runnable			the RunnableWithExceptions to be attempted
	 * @throws Exception		It can be any exception thrown from within the Runnable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws TooManyAttemptsException  If there is no exception happened during last attempt but there is a TooManyAttemptsException
	 * @throws InterruptedException  If there is no exception happened during last attempt but there is a InterruptedException
     */
    public void run(RunnableWithExceptions runnable){
       	ExceptionUncheckUtility.uncheck(()->runThrowingSuppressed(runnable));
    }
    


    /**
     * Constructor initiating an empty strategy ready for further configuring.
     */
    public AttemptStrategy(){
    	super();
    }
    
    /**
     * Constructor initiating an strategy by copying configurations from another instance.
     * @param that		another attempt strategy
     */
    public AttemptStrategy(AttemptStrategyImpl that){
    	super(that);
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
    public AttemptStrategy withBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy) throws IllegalStateException {
        return (AttemptStrategy) super.withBackoffStrategy(backoffStrategy);
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

    
    ///////////  switching to CallableAttemptStrategy<R>
    
    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return a new instance of CallableAttemptStrategy with the same configuration as this
     */
    public <R> CallableAttemptStrategy<R> retryIfAttemptHasResult(@Nonnull Predicate<Attempt<R>> resultPredicate) {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfAttemptHasResult(resultPredicate);
    }

    /**
     * Adds a predicate to decide whether next attempt is needed when there is a result from previous attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return a new instance of CallableAttemptStrategy with the same configuration as this
     */
    public <R> CallableAttemptStrategy<R> retryIfResult(@Nonnull Predicate<R> resultPredicate) {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfResult(resultPredicate);
    }

    /**
     * Adds a predicate to make sure that if there is a null result from an attempt there will be a need for next attempt.
     * retryIf*Result*(...) methods can be called multiple times, all the predicates will be or-ed.
     * If no retryIf*Result*(...) method has been called or if a result got from an attempt cannot
     * make any of the predicates true, it will be returned and there will be no further attempt.
     * @param resultPredicate	The predicate to be called when a result was returned in previous attempt.
     * 								It should return true if another attempt is needed.
     * @return a new instance of CallableAttemptStrategy with the same configuration as this
     */
     public <R> CallableAttemptStrategy<R> retryIfResultIsNull() {
    	CallableAttemptStrategy<R> that = new CallableAttemptStrategy<R>(this);
    	return that.retryIfResultIsNull();
    }

    /**
     * Switch to CallableAttemptStrategy which has a call(...) method
     * @return	a new instance of CallableAttemptStrategy with the same configuration as this
     */
    public <R> CallableAttemptStrategy<R>  toCallableAttemptStrategy(){
    	return new CallableAttemptStrategy<R>(this);
    }
    
	/**
	 * Attempt the Callable according to the strategies defined, throwing all the exceptions in their original forms.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws TooManyAttemptsException				If no more attempt is allowed by the stop strategy
	 * @throws InterruptedBeforeAttemptException	If InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
	 */
    public <R> R callThrowingAll(Callable<R> callable)
    		throws TooManyAttemptsException, InterruptedBeforeAttemptException, Exception {
    	return new CallableAttemptStrategy<R>(this).callThrowingAll(callable);
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
    public <R> R callThrowingUncheckedAll(Callable<R> callable){
    	return new CallableAttemptStrategy<R>(this).callThrowingUncheckedAll(callable);
    }

    /**
     * Attempt the Callable according to the strategies defined, throwing exceptions in their original or modified forms.
     * If no more attempt is allowed by the stop strategy or InterruptedException happened while applying attempt time limit strategy or backoff strategy,
     * a TooManyAttemptsException or InterruptedException will be added as a suppressed exception to the exception thrown from within the last attempt.
	 * @param callable			the Callable to be attempted
	 * @return					the result returned by the Callable
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies.
	 * 							Optionally with a TooManyAttemptsException or InterruptedException as one of its suppressed exceptions.
	 * @throws TooManyAttemptsException  If there is no exception happened during last attempt but there is a TooManyAttemptsException
	 * @throws InterruptedException  If there is no exception happened during last attempt but there is a InterruptedException
     */
    public <R> R callThrowingSuppressed(Callable<R> callable) throws Exception, TooManyAttemptsException, InterruptedException{
    	return new CallableAttemptStrategy<R>(this).callThrowingSuppressed(callable);
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
    public <R> R call(Callable<R> callable){
    	return new CallableAttemptStrategy<R>(this).call(callable);
    }
 

}
