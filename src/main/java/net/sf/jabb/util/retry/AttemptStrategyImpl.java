/*
 * Copyright 2012-2015 Ray Holder
 * Copyright 2015 James Hu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sf.jabb.util.retry;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.BackoffStrategy;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.parallel.WaitStrategy;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

/**
 * Base class with core functions of AttemptStrategy
 *
 * @author JB
 * @author Jason Dunkelberger (dirkraft)
 * @author James Hu
 */
class AttemptStrategyImpl {
	protected StopStrategy stopStrategy;
	protected AttemptBackoffStrategy backoffStrategy;
	protected WaitStrategy waitStrategy;
	protected TimeLimiter attemptTimeLimiter;
	protected Duration attemptTimeLimit;
	protected Predicate<Attempt<Void>> retryConditionOnExceptions;
	protected List<AttemptListener> listeners;
    
    protected AttemptStrategyImpl(){
    }

    protected AttemptStrategyImpl(AttemptStrategyImpl that){
    	this.stopStrategy = that.stopStrategy;
    	this.backoffStrategy = that.backoffStrategy;
    	this.waitStrategy = that.waitStrategy;
    	this.attemptTimeLimiter = that.attemptTimeLimiter;
    	this.attemptTimeLimit = that.attemptTimeLimit;
    	this.retryConditionOnExceptions = that.retryConditionOnExceptions;
    	if (that.listeners != null){
    		this.listeners = new LinkedList<>(that.listeners);
    	}
    }
    
    protected void setDefaultsIfNotSet(){
    	if (stopStrategy == null){
    		stopStrategy = StopStrategies.neverStop();
    	}
    	if (backoffStrategy == null){
    		backoffStrategy = AttemptBackoffStrategies.simpleBackoff(BackoffStrategies.noBackoff());
    	}
    	if (waitStrategy == null){
    		waitStrategy = WaitStrategies.threadSleepStrategy();
    	}
    }
    
    /**
     * Executes the given callable. If the rejection predicate
     * accepts the attempt, the stop strategy is used to decide if a new attempt
     * must be made. Then the wait strategy is used to decide how much time to sleep
     * and a new attempt is made.
     * @param <V> type of the result of the attempt
     * @param callable 					the callable task to be executed
     * @param retryConditionOnResult	retry condition based on the result
     * @return the computed result of the given callable
	 * @throws AttemptException		Any exception happened while applying the attempt strategy.
	 * 								For example, {@link TooManyAttemptsException} if no more attempt is allowed by the stop strategy,
	 * 								or {@link InterruptedBeforeAttemptException} if InterruptedException happened while applying attempt time limit strategy or backoff strategy
	 * @throws Exception		It can be any exception thrown from within the Callable that is considered as non-recoverable by the retry strategies
     */
    protected <V> V callThrowingAll(Callable<V> callable, Predicate<Attempt<V>> retryConditionOnResult) 
    		throws AttemptException, Exception {
    	setDefaultsIfNotSet();
    	
        Attempt<?> attempt = null;
        Attempt<V> attemptWithResult = null;
        Attempt<Void> attemptWithException = null;

        Object context = null;
        Instant firstStartTime = Instant.now();
        for (int attemptNumber = 1; ; attemptNumber++) {
            try {
                V result = attemptTimeLimiter == null ? callable.call() 
                		: attemptTimeLimiter.callWithTimeout(callable, attemptTimeLimit.toMillis(), TimeUnit.MILLISECONDS, true);
                attemptWithResult = Attempt.withResult(context, attemptNumber, firstStartTime, Instant.now(), result);
                attempt = attemptWithResult;
            } catch (Exception t) {
            	attemptWithException = Attempt.withException(context, attemptNumber, firstStartTime, Instant.now(), t);
            	attempt = attemptWithException;
            }

            if (listeners != null){
                for (AttemptListener listener : listeners) {
                    listener.onAttempted(attempt);
                }
                context = attempt.getContext();		// it may have been changed from inside the listeners
            }

            if (attempt.hasException()){	// no result
            	if (attemptTimeLimiter != null && attempt.getException() instanceof UncheckedTimeoutException 	// always retry in this situation
            			|| retryConditionOnExceptions != null && retryConditionOnExceptions.test(attemptWithException)) {
            		// should retry
            	} else { // should abort
            		throw attempt.getException();
            	}
            }else{	// has a result
            	if(retryConditionOnResult != null && retryConditionOnResult.test(attemptWithResult)){
            		// should retry
            	}else{	// should return
                	return attemptWithResult.getResult();
            	}
            }
            
            if (stopStrategy.shouldStop(attempt)) {
                throw new TooManyAttemptsException(attempt);
            } else {
                long sleepTime = backoffStrategy.computeBackoffMilliseconds(attempt);
                try {
                    waitStrategy.await(sleepTime);
                } catch (InterruptedException e) {
                	waitStrategy.handleInterruptedException(e);
                    throw new InterruptedBeforeAttemptException(e, attempt);
                }
            }
        }
    }
    
    
    protected void addAttemptListener(AttemptListener listener) {
        Preconditions.checkNotNull(listener, "listener may not be null");
        if (listeners == null){
        	listeners = new LinkedList<>();
        }
        listeners.add(listener);
    }

	protected void setStopStrategy(StopStrategy stopStrategy) {
        Preconditions.checkNotNull(stopStrategy, "stopStrategy may not be null");
		this.stopStrategy = stopStrategy;
	}

	protected void setBackoffStrategy(AttemptBackoffStrategy backoffStrategy) {
        Preconditions.checkNotNull(backoffStrategy, "backoffStrategy may not be null");
		this.backoffStrategy = backoffStrategy;
	}

	protected void setWaitStrategy(WaitStrategy waitStrategy) {
        Preconditions.checkNotNull(waitStrategy, "waitStrategy may not be null");
		this.waitStrategy = waitStrategy;
	}

	protected void setAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        Preconditions.checkNotNull(attemptTimeLimiter, "attemptTimeLimiter may not be null");
        Preconditions.checkNotNull(attemptTimeLimit, "attemptTimeLimit may not be null");
        Preconditions.checkArgument(!attemptTimeLimit.isNegative() && !attemptTimeLimit.isZero(), "attemptTimeLimit may not be negative or zero");
		this.attemptTimeLimiter = attemptTimeLimiter;
		this.attemptTimeLimit = attemptTimeLimit;
	}

	/////////////
	
	protected AttemptStrategyImpl withStopStrategy(@Nonnull StopStrategy stopStrategy) throws IllegalStateException {
        Preconditions.checkState(this.stopStrategy == null, "a stop strategy has already been set %s", this.stopStrategy);
        this.setStopStrategy(stopStrategy);
        return this;
    }
	
	protected AttemptStrategyImpl overrideStopStrategy(@Nonnull StopStrategy stopStrategy){
        this.setStopStrategy(stopStrategy);
        return this;
	}

    protected AttemptStrategyImpl withBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy) throws IllegalStateException {
        Preconditions.checkState(this.backoffStrategy == null, "a backoff strategy has already been set %s", this.backoffStrategy);
        this.setBackoffStrategy(backoffStrategy);
        return this;
    }

    protected AttemptStrategyImpl overrideBackoffStrategy(@Nonnull AttemptBackoffStrategy backoffStrategy){
        this.setBackoffStrategy(backoffStrategy);
        return this;
    }

    protected AttemptStrategyImpl withBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy) throws IllegalStateException {
        Preconditions.checkState(this.backoffStrategy == null, "a backoff strategy has already been set %s", this.backoffStrategy);
        this.setBackoffStrategy(AttemptBackoffStrategies.simpleBackoff(backoffStrategy));
        return this;
    }

    protected AttemptStrategyImpl overrideBackoffStrategy(@Nonnull BackoffStrategy backoffStrategy){
        this.setBackoffStrategy(AttemptBackoffStrategies.simpleBackoff(backoffStrategy));
        return this;
    }

    protected AttemptStrategyImpl withWaitStrategy(@Nonnull WaitStrategy waitStrategy) throws IllegalStateException {
        Preconditions.checkState(this.waitStrategy == null, "a wait strategy has already been set %s", this.backoffStrategy);
        this.setWaitStrategy(waitStrategy);
        return this;
    }
    
    protected AttemptStrategyImpl overrideWaitStrategy(@Nonnull WaitStrategy waitStrategy){
        this.setWaitStrategy(waitStrategy);
        return this;
    }
    
    protected AttemptStrategyImpl withAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit) throws IllegalStateException {
        Preconditions.checkState(this.attemptTimeLimit == null, "an attempt time limit has already been set %s", this.attemptTimeLimit);
        setAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
        return this;
    }

    protected AttemptStrategyImpl overrideAttemptTimeLimit(TimeLimiter attemptTimeLimiter, Duration attemptTimeLimit){
        setAttemptTimeLimit(attemptTimeLimiter, attemptTimeLimit);
        return this;
    }

    protected AttemptStrategyImpl withAttemptListener(@Nonnull AttemptListener listener) {
    	this.addAttemptListener(listener);
        return this;
    }
    
    protected AttemptStrategyImpl retryIfAttemptHasException(@Nonnull Predicate<Attempt<Void>> exceptionPredicate) {
        Preconditions.checkNotNull(exceptionPredicate, "exceptionPredicate may not be null");
        retryConditionOnExceptions = retryConditionOnExceptions == null ? exceptionPredicate
        		: retryConditionOnExceptions.or(exceptionPredicate);
        return this;
    }

    protected AttemptStrategyImpl retryIfException(@Nonnull Predicate<Exception> exceptionPredicate) {
        return retryIfAttemptHasException(attempt->exceptionPredicate.test(attempt.getException()));
    }

    @SuppressWarnings("unchecked")
	protected <E extends Exception> AttemptStrategyImpl retryIfException(@Nonnull Class<E> exceptionClass, @Nonnull Predicate<E> exceptionPredicate) {
        return retryIfAttemptHasException(attempt->{
        	Exception e = attempt.getException();
        	if (exceptionClass.isAssignableFrom(attempt.getException().getClass())){
        		return exceptionPredicate.test((E)e);
        	}else{
        		return false;
        	}
        });
    }

    protected AttemptStrategyImpl retryIfException(@Nonnull Class<? extends Exception> exceptionClass) {
        Preconditions.checkNotNull(exceptionClass, "exception class may not be null");
        return retryIfAttemptHasException(new ExceptionClassPredicate(exceptionClass));
    }

    protected AttemptStrategyImpl retryIfRuntimeException() {
        return retryIfAttemptHasException(new ExceptionClassPredicate(RuntimeException.class));
    }

    protected AttemptStrategyImpl retryIfException() {
        return retryIfAttemptHasException(X->true);
    }

    private static final class ExceptionClassPredicate implements Predicate<Attempt<Void>> {

        private Class<? extends Exception> exceptionClass;

        public ExceptionClassPredicate(Class<? extends Exception> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        @Override
        public boolean test(Attempt<Void> attempt) {
            return exceptionClass.isAssignableFrom(attempt.getException().getClass());
        }
    }


}
