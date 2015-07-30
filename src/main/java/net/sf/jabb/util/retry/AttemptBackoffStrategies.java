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

import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import net.sf.jabb.util.parallel.BackoffStrategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Factory class for instances of {@link AttemptBackoffStrategy}.
 *
 * @author JB
 * @author James Hu
 */
public final class AttemptBackoffStrategies {

    private AttemptBackoffStrategies() {
    }

    public static AttemptBackoffStrategy simpleBackoff(BackoffStrategy backoffStrategy){
        Preconditions.checkNotNull(backoffStrategy, "backoffStrategy may not be null");
        return new SimpleBackoffStrategy(backoffStrategy);
    }

    /**
     * Returns a strategy which waits for an amount of time based on the Exception that occurred. The
     * {@code function} determines how the wait time should be calculated for the given
     * {@code exceptionClass}. If the exception does not match, a wait time of 0 is returned.
     *
     * @param function       function to calculate wait time
     * @param exceptionClass class to calculate wait time from
     * @return a backoff strategy calculated from the failed attempt
     */
    public static <T extends Exception> AttemptBackoffStrategy exceptionBasedBackoff(@Nonnull Class<T> exceptionClass,
                                                                   @Nonnull Function<T, Long> function) {
        Preconditions.checkNotNull(exceptionClass, "exceptionClass may not be null");
        Preconditions.checkNotNull(function, "function may not be null");
        return new ExceptionBasedBackoffStrategy<T>(exceptionClass, function);
    }

    /**
     * Joins one or more wait strategies to derive a composite backoff strategy.
     * The new joined strategy will have a wait time which is total of all wait times computed one after another in order.
     *
     * @param waitStrategies Wait strategies that need to be applied one after another for computing the wait time.
     * @return A composite backoff strategy
     */
    public static AttemptBackoffStrategy join(AttemptBackoffStrategy... waitStrategies) {
        Preconditions.checkState(waitStrategies.length > 0, "Must have at least one backoff strategy");
        List<AttemptBackoffStrategy> waitStrategyList = Lists.newArrayList(waitStrategies);
        Preconditions.checkState(!waitStrategyList.contains(null), "Cannot have a null backoff strategy");
        return new CompositeBackoffStrategy(waitStrategyList);
    }

    @Immutable
    private static final class SimpleBackoffStrategy implements AttemptBackoffStrategy {
    	private final BackoffStrategy backoffStrategy;
    	
    	public SimpleBackoffStrategy(BackoffStrategy backoffStrategy){
            Preconditions.checkNotNull(backoffStrategy, "backoffStrategy may not be null");
    		this.backoffStrategy = backoffStrategy;
    	}

		@Override
		public <T> long computeBackoffMilliseconds(Attempt<T> attempt) {
			return backoffStrategy.computeBackoffMilliseconds(attempt.getTotalAttempts());
		}
    }

    @Immutable
    private static final class CompositeBackoffStrategy implements AttemptBackoffStrategy {
        private final List<AttemptBackoffStrategy> waitStrategies;

        public CompositeBackoffStrategy(List<AttemptBackoffStrategy> waitStrategies) {
            Preconditions.checkState(!waitStrategies.isEmpty(), "Need at least one backoff strategy");
            this.waitStrategies = waitStrategies;
        }

        @Override
        public <R> long computeBackoffMilliseconds(Attempt<R> failedAttempt) {
            long waitTime = 0L;
            for (AttemptBackoffStrategy waitStrategy : waitStrategies) {
                waitTime += waitStrategy.computeBackoffMilliseconds(failedAttempt);
            }
            return waitTime;
        }
    }

    @Immutable
    private static final class ExceptionBasedBackoffStrategy<T extends Exception> implements AttemptBackoffStrategy {
        private final Class<T> exceptionClass;
        private final Function<T, Long> function;

        public ExceptionBasedBackoffStrategy(@Nonnull Class<T> exceptionClass, @Nonnull Function<T, Long> function) {
            this.exceptionClass = exceptionClass;
            this.function = function;
        }

        @SuppressWarnings("unchecked")
		@Override
        public <R> long computeBackoffMilliseconds(Attempt<R> lastAttempt) {
            if (lastAttempt.hasException()) {
                Exception cause = lastAttempt.getException();
                if (exceptionClass.isAssignableFrom(cause.getClass())) {
                    return function.apply((T) cause);
                }
            }
            return 0L;
        }
    }
}
