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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Factory class for instances of {@link BackoffStrategy}.
 *
 * @author JB
 * @author James Hu
 */
public final class BackoffStrategies {

    private static final BackoffStrategy NO_WAIT_STRATEGY = new FixedDurationBackoffStrategy(0L);

    private BackoffStrategies() {
    }

    /**
     * Returns a backoff strategy that doesn't wait at all before retrying. Use this at your own risk.
     *
     * @return a backoff strategy that doesn't wait between retries
     */
    public static BackoffStrategy noWait() {
        return NO_WAIT_STRATEGY;
    }

    /**
     * Returns a backoff strategy that waits a fixed amount of time before retrying.
     *
     * @param sleepTime the time to sleep
     * @param timeUnit  the unit of the time to sleep
     * @return a backoff strategy that waits a fixed amount of time
     * @throws IllegalStateException if the wait time is &lt; 0
     */
    public static BackoffStrategy fixedWait(long sleepTime, @Nonnull TimeUnit timeUnit) throws IllegalStateException {
        Preconditions.checkNotNull(timeUnit, "The time unit may not be null");
        return new FixedDurationBackoffStrategy(timeUnit.toMillis(sleepTime));
    }

    /**
     * Returns a backoff strategy that waits a fixed amount of time before retrying.
     *
     * @param waitDuration the time duration to wait
     * @return a backoff strategy that waits a fixed amount of time
     * @throws IllegalStateException if the wait time is &lt; 0
     */
    public static BackoffStrategy fixedWait(Duration waitDuration) throws IllegalStateException {
        Preconditions.checkNotNull(waitDuration, "The duration may not be null");
        return new FixedDurationBackoffStrategy(waitDuration.toMillis());
    }

    /**
     * Returns a strategy that waits a random amount of time before retrying.
     *
     * @param maximumTime the maximum time to sleep
     * @param timeUnit    the unit of the maximum time
     * @return a backoff strategy with a random wait time
     * @throws IllegalStateException if the maximum wait time is &lt;= 0.
     */
    public static BackoffStrategy randomWait(long maximumTime, @Nonnull TimeUnit timeUnit) {
        Preconditions.checkNotNull(timeUnit, "The time unit may not be null");
        return new RandomWaitStrategy(0L, timeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy that waits a random amount of time before retrying.
     *
     * @param minimumTime     the minimum time to sleep
     * @param minimumTimeUnit the unit of the minimum time
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a backoff strategy with a random wait time
     * @throws IllegalStateException if the minimum wait time is &lt; 0, or if the
     *                               maximum wait time is less than (or equals to) the minimum.
     */
    public static BackoffStrategy randomWait(long minimumTime,
                                          @Nonnull TimeUnit minimumTimeUnit,
                                          long maximumTime,
                                          @Nonnull TimeUnit maximumTimeUnit) {
        Preconditions.checkNotNull(minimumTimeUnit, "The minimum time unit may not be null");
        Preconditions.checkNotNull(maximumTimeUnit, "The maximum time unit may not be null");
        return new RandomWaitStrategy(minimumTimeUnit.toMillis(minimumTime),
                maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy that waits a random amount of time before retrying.
     *
     * @param maxDuration the maximum duration to sleep
     * @return a backoff strategy with a random wait time
     * @throws IllegalStateException if the maximum wait time is &lt;= 0.
     */
    public static BackoffStrategy randomWait(Duration maxDuration) {
        Preconditions.checkNotNull(maxDuration, "The maxDuration may not be null");
        return new RandomWaitStrategy(0L, maxDuration.toMillis());
    }

    /**
     * Returns a strategy that waits a random amount of time before retrying.
     *
     * @param minDuration the minimum duration to sleep
     * @param maxDuration the maximum duration to sleep
     * @return a backoff strategy with a random wait time
     * @throws IllegalStateException if the minimum wait time is &lt; 0, or if the
     *                               maximum wait time is less than (or equals to) the minimum.
     */
    public static BackoffStrategy randomWait(Duration minDuration, Duration maxDuration){
        Preconditions.checkNotNull(minDuration, "The minimum duration may not be null");
        Preconditions.checkNotNull(maxDuration, "The maximum duration may not be null");
        return new RandomWaitStrategy(minDuration.toMillis(), maxDuration.toMillis());
    }

    /**
     * Returns a strategy that waits a fixed amount of time after the first
     * failed attempt and in incrementing amounts of time after each additional
     * failed attempt.
     *
     * @param initialSleepTime     the time to wait before retrying the first time
     * @param initialSleepTimeUnit the unit of the initial wait time
     * @param increment            the increment added to the previous wait time after each failed attempt
     * @param incrementTimeUnit    the unit of the increment
     * @return a backoff strategy that incrementally waits an additional fixed time after each failed attempt
     */
    public static BackoffStrategy incrementingWait(long initialSleepTime,
                                                @Nonnull TimeUnit initialSleepTimeUnit,
                                                long increment,
                                                @Nonnull TimeUnit incrementTimeUnit) {
        Preconditions.checkNotNull(initialSleepTimeUnit, "The initial wait time unit may not be null");
        Preconditions.checkNotNull(incrementTimeUnit, "The increment time unit may not be null");
        return new IncrementingWaitStrategy(initialSleepTimeUnit.toMillis(initialSleepTime),
                incrementTimeUnit.toMillis(increment));
    }

    /**
     * Returns a strategy that waits a fixed amount of time after the first
     * failed attempt and in incrementing amounts of time after each additional
     * failed attempt.
     *
     * @param initialSleepTime     the time to wait before retrying the first time
     * @param initialSleepTimeUnit the unit of the initial wait time
     * @param increment            the increment added to the previous wait time after each failed attempt
     * @param incrementTimeUnit    the unit of the increment
     * @return a backoff strategy that incrementally waits an additional fixed time after each failed attempt
     */
    public static BackoffStrategy incrementingWait(@Nonnull Duration initialDuration,
                                                @Nonnull Duration incrementDuration) {
        Preconditions.checkNotNull(initialDuration, "The initial wait duration may not be null");
        Preconditions.checkNotNull(incrementDuration, "The increment wait duration may not be null");
        return new IncrementingWaitStrategy(initialDuration.toMillis(),	incrementDuration.toMillis());
    }

    /**
     * Returns a strategy which waits for an exponential amount of time after the first failed attempt,
     * and in exponentially incrementing amounts after each failed attempt up to Long.MAX_VALUE.
     *
     * @return a backoff strategy that increments with each failed attempt using exponential backoff
     */
    public static BackoffStrategy exponentialWait() {
        return new ExponentialWaitStrategy(1, Long.MAX_VALUE);
    }

    /**
     * Returns a strategy which waits for an exponential amount of time after the first failed attempt,
     * and in exponentially incrementing amounts after each failed attempt up to the maximumTime.
     *
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a backoff strategy that increments with each failed attempt using exponential backoff
     */
    public static BackoffStrategy exponentialWait(long maximumTime,
                                               @Nonnull TimeUnit maximumTimeUnit) {
        Preconditions.checkNotNull(maximumTimeUnit, "The maximum time unit may not be null");
        return new ExponentialWaitStrategy(1, maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy which waits for an exponential amount of time after the first failed attempt,
     * and in exponentially incrementing amounts after each failed attempt up to the maximumTime.
     *
     * @param maximumDuration     the maximum time to wait
     * @return a backoff strategy that increments with each failed attempt using exponential backoff
     */
    public static BackoffStrategy exponentialWait(@Nonnull Duration maximumDuration) {
        Preconditions.checkNotNull(maximumDuration, "The maximum duration may not be null");
        return new ExponentialWaitStrategy(1, maximumDuration.toMillis());
    }

    /**
     * Returns a strategy which waits for an exponential amount of time after the first failed attempt,
     * and in exponentially incrementing amounts after each failed attempt up to the maximumTime.
     * The wait time between the retries can be controlled by the multiplier.
     * nextWaitTime = exponentialIncrement * {@code multiplier}.
     *
     * @param multiplier      multiply the wait time calculated by this
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a backoff strategy that increments with each failed attempt using exponential backoff
     */
    public static BackoffStrategy exponentialWait(long multiplier,
                                               long maximumTime,
                                               @Nonnull TimeUnit maximumTimeUnit) {
        Preconditions.checkNotNull(maximumTimeUnit, "The maximum time unit may not be null");
        return new ExponentialWaitStrategy(multiplier, maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy which waits for an exponential amount of time after the first failed attempt,
     * and in exponentially incrementing amounts after each failed attempt up to the maximumTime.
     * The wait time between the retries can be controlled by the multiplier.
     * nextWaitTime = exponentialIncrement * {@code multiplier}.
     *
     * @param multiplier      multiply the wait time calculated by this
     * @param maximumDuration     the maximum time to wait
     * @return a backoff strategy that increments with each failed attempt using exponential backoff
     */
    public static BackoffStrategy exponentialWait(long multiplier, @Nonnull Duration maximumDuration) {
        Preconditions.checkNotNull(maximumDuration, "The maximum duration may not be null");
        return new ExponentialWaitStrategy(multiplier, maximumDuration.toMillis());
    }

    /**
     * Returns a strategy which waits for an increasing amount of time after the first failed attempt,
     * and in Fibonacci increments after each failed attempt up to {@link Long#MAX_VALUE}.
     *
     * @return a backoff strategy that increments with each failed attempt using a Fibonacci sequence
     */
    public static BackoffStrategy fibonacciWait() {
        return new FibonacciWaitStrategy(1, Long.MAX_VALUE);
    }

    /**
     * Returns a strategy which waits for an increasing amount of time after the first failed attempt,
     * and in Fibonacci increments after each failed attempt up to the {@code maximumTime}.
     *
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a backoff strategy that increments with each failed attempt using a Fibonacci sequence
     */
    public static BackoffStrategy fibonacciWait(long maximumTime,
                                             @Nonnull TimeUnit maximumTimeUnit) {
        Preconditions.checkNotNull(maximumTimeUnit, "The maximum time unit may not be null");
        return new FibonacciWaitStrategy(1, maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy which waits for an increasing amount of time after the first failed attempt,
     * and in Fibonacci increments after each failed attempt up to the {@code maximumTime}.
     *
     * @param maximumDuration     the maximum time to wait
     * @return a backoff strategy that increments with each failed attempt using a Fibonacci sequence
     */
    public static BackoffStrategy fibonacciWait(@Nonnull Duration maximumDuration) {
        Preconditions.checkNotNull(maximumDuration, "The maximum duration may not be null");
        return new FibonacciWaitStrategy(1, maximumDuration.toMillis());
    }

    /**
     * Returns a strategy which waits for an increasing amount of time after the first failed attempt,
     * and in Fibonacci increments after each failed attempt up to the {@code maximumTime}.
     * The wait time between the retries can be controlled by the multiplier.
     * nextWaitTime = fibonacciIncrement * {@code multiplier}.
     *
     * @param multiplier      multiply the wait time calculated by this
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a backoff strategy that increments with each failed attempt using a Fibonacci sequence
     */
    public static BackoffStrategy fibonacciWait(long multiplier, long maximumTime,
            @Nonnull TimeUnit maximumTimeUnit) {
        Preconditions.checkNotNull(maximumTimeUnit, "The maximum time unit may not be null");
        return new FibonacciWaitStrategy(multiplier, maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy which waits for an increasing amount of time after the first failed attempt,
     * and in Fibonacci increments after each failed attempt up to the {@code maximumTime}.
     * The wait time between the retries can be controlled by the multiplier.
     * nextWaitTime = fibonacciIncrement * {@code multiplier}.
     *
     * @param multiplier      multiply the wait time calculated by this
     * @param maximumDuration     the maximum time to wait
     * @return a backoff strategy that increments with each failed attempt using a Fibonacci sequence
     */
    public static BackoffStrategy fibonacciWait(long multiplier, @Nonnull Duration maximumDuration) {
        Preconditions.checkNotNull(maximumDuration, "The maximum duration may not be null");
        return new FibonacciWaitStrategy(multiplier, maximumDuration.toMillis());
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
    public static <T extends Throwable> BackoffStrategy exceptionWait(@Nonnull Class<T> exceptionClass,
                                                                   @Nonnull Function<T, Long> function) {
        Preconditions.checkNotNull(exceptionClass, "exceptionClass may not be null");
        Preconditions.checkNotNull(function, "function may not be null");
        return new ExceptionWaitStrategy<T>(exceptionClass, function);
    }

    /**
     * Joins one or more wait strategies to derive a composite backoff strategy.
     * The new joined strategy will have a wait time which is total of all wait times computed one after another in order.
     *
     * @param waitStrategies Wait strategies that need to be applied one after another for computing the wait time.
     * @return A composite backoff strategy
     */
    public static BackoffStrategy join(BackoffStrategy... waitStrategies) {
        Preconditions.checkState(waitStrategies.length > 0, "Must have at least one backoff strategy");
        List<BackoffStrategy> waitStrategyList = Lists.newArrayList(waitStrategies);
        Preconditions.checkState(!waitStrategyList.contains(null), "Cannot have a null backoff strategy");
        return new CompositeWaitStrategy(waitStrategyList);
    }

    @Immutable
    private static final class FixedDurationBackoffStrategy implements BackoffStrategy {
        private final long sleepTime;

        public FixedDurationBackoffStrategy(long sleepTime) {
            Preconditions.checkArgument(sleepTime >= 0L, "sleepTime must be >= 0 but is %d", sleepTime);
            this.sleepTime = sleepTime;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            return sleepTime;
        }
    }

    @Immutable
    private static final class RandomWaitStrategy implements BackoffStrategy {
        private static final Random RANDOM = new Random();
        private final long minimum;
        private final long maximum;

        public RandomWaitStrategy(long minimum, long maximum) {
            Preconditions.checkArgument(minimum >= 0, "minimum must be >= 0 but is %d", minimum);
            Preconditions.checkArgument(maximum > minimum, "maximum must be > minimum but maximum is %d and minimum is", maximum, minimum);

            this.minimum = minimum;
            this.maximum = maximum;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            long t = Math.abs(RANDOM.nextLong()) % (maximum - minimum);
            return t + minimum;
        }
    }

    @Immutable
    private static final class IncrementingWaitStrategy implements BackoffStrategy {
        private final long initialSleepTime;
        private final long increment;

        public IncrementingWaitStrategy(long initialSleepTime,
                                        long increment) {
            Preconditions.checkArgument(initialSleepTime >= 0L, "initialSleepTime must be >= 0 but is %d", initialSleepTime);
            this.initialSleepTime = initialSleepTime;
            this.increment = increment;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            long result = initialSleepTime + (increment * (failedAttempt.getTotalAttempts() - 1));
            return result >= 0L ? result : 0L;
        }
    }

    @Immutable
    private static final class ExponentialWaitStrategy implements BackoffStrategy {
        private final long multiplier;
        private final long maximumWait;

        public ExponentialWaitStrategy(long multiplier,
                                       long maximumWait) {
            Preconditions.checkArgument(multiplier > 0L, "multiplier must be > 0 but is %d", multiplier);
            Preconditions.checkArgument(maximumWait >= 0L, "maximumWait must be >= 0 but is %d", maximumWait);
            Preconditions.checkArgument(multiplier < maximumWait, "multiplier must be < maximumWait but is %d", multiplier);
            this.multiplier = multiplier;
            this.maximumWait = maximumWait;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            double exp = Math.pow(2, failedAttempt.getTotalAttempts());
            long result = Math.round(multiplier * exp);
            if (result > maximumWait) {
                result = maximumWait;
            }
            return result >= 0L ? result : 0L;
        }
    }

    @Immutable
    private static final class FibonacciWaitStrategy implements BackoffStrategy {
        private final long multiplier;
        private final long maximumWait;

        public FibonacciWaitStrategy(long multiplier, long maximumWait) {
            Preconditions.checkArgument(multiplier > 0L, "multiplier must be > 0 but is %d", multiplier);
            Preconditions.checkArgument(maximumWait >= 0L, "maximumWait must be >= 0 but is %d", maximumWait);
            Preconditions.checkArgument(multiplier < maximumWait, "multiplier must be < maximumWait but is %d", multiplier);
            this.multiplier = multiplier;
            this.maximumWait = maximumWait;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            long fib = fib(failedAttempt.getTotalAttempts());
            long result = multiplier * fib;

            if (result > maximumWait || result < 0L) {
                result = maximumWait;
            }

            return result >= 0L ? result : 0L;
        }

        private long fib(long n) {
            if (n == 0L) return 0L;
            if (n == 1L) return 1L;

            long prevPrev = 0L;
            long prev = 1L;
            long result = 0L;

            for (long i = 2L; i <= n; i++) {
                result = prev + prevPrev;
                prevPrev = prev;
                prev = result;
            }

            return result;
        }
    }

    @Immutable
    private static final class CompositeWaitStrategy implements BackoffStrategy {
        private final List<BackoffStrategy> waitStrategies;

        public CompositeWaitStrategy(List<BackoffStrategy> waitStrategies) {
            Preconditions.checkState(!waitStrategies.isEmpty(), "Need at least one backoff strategy");
            this.waitStrategies = waitStrategies;
        }

        @Override
        public long computeBackoffMilliseconds(Attempt failedAttempt) {
            long waitTime = 0L;
            for (BackoffStrategy waitStrategy : waitStrategies) {
                waitTime += waitStrategy.computeBackoffMilliseconds(failedAttempt);
            }
            return waitTime;
        }
    }

    @Immutable
    private static final class ExceptionWaitStrategy<T extends Throwable> implements BackoffStrategy {
        private final Class<T> exceptionClass;
        private final Function<T, Long> function;

        public ExceptionWaitStrategy(@Nonnull Class<T> exceptionClass, @Nonnull Function<T, Long> function) {
            this.exceptionClass = exceptionClass;
            this.function = function;
        }

        @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ConstantConditions", "unchecked"})
        @Override
        public long computeBackoffMilliseconds(Attempt lastAttempt) {
            if (lastAttempt.hasException()) {
                Throwable cause = lastAttempt.getException();
                if (exceptionClass.isAssignableFrom(cause.getClass())) {
                    return function.apply((T) cause);
                }
            }
            return 0L;
        }
    }
}
