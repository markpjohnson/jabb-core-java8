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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import java.util.function.Function;
import com.google.common.collect.Sets;

public class BackoffStrategiesTest {


    @Test
    public void testNoWait() {
        BackoffStrategy noWait = BackoffStrategies.noWait();
        assertEquals(0L, noWait.computeBackoffMilliseconds(failedAttempt(18, 9879L)));
    }

    @Test
    public void testFixedWait() {
        BackoffStrategy fixedWait = BackoffStrategies.fixedWait(1000L, TimeUnit.MILLISECONDS);
        assertEquals(1000L, fixedWait.computeBackoffMilliseconds(failedAttempt(12, 6546L)));
    }

    @Test
    public void testIncrementingWait() {
        BackoffStrategy incrementingWait = BackoffStrategies.incrementingWait(500L, TimeUnit.MILLISECONDS, 100L, TimeUnit.MILLISECONDS);
        assertEquals(500L, incrementingWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        assertEquals(600L, incrementingWait.computeBackoffMilliseconds(failedAttempt(2, 6546L)));
        assertEquals(700L, incrementingWait.computeBackoffMilliseconds(failedAttempt(3, 6546L)));
    }

    @Test
    public void testRandomWait() {
        BackoffStrategy randomWait = BackoffStrategies.randomWait(1000L, TimeUnit.MILLISECONDS, 2000L, TimeUnit.MILLISECONDS);
        Set<Long> times = Sets.newHashSet();
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        assertTrue(times.size() > 1); // if not, the random is not random
        for (long time : times) {
            assertTrue(time >= 1000L);
            assertTrue(time <= 2000L);
        }
    }

    @Test
    public void testRandomWaitWithoutMinimum() {
        BackoffStrategy randomWait = BackoffStrategies.randomWait(2000L, TimeUnit.MILLISECONDS);
        Set<Long> times = Sets.newHashSet();
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        times.add(randomWait.computeBackoffMilliseconds(failedAttempt(1, 6546L)));
        assertTrue(times.size() > 1); // if not, the random is not random
        for (long time : times) {
            assertTrue(time >= 0L);
            assertTrue(time <= 2000L);
        }
    }

    @Test
    public void testExponential() {
        BackoffStrategy exponentialWait = BackoffStrategies.exponentialWait();
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(1, 0)) == 2);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(2, 0)) == 4);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(3, 0)) == 8);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(4, 0)) == 16);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(5, 0)) == 32);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(6, 0)) == 64);
    }

    @Test
    public void testExponentialWithMaximumWait() {
        BackoffStrategy exponentialWait = BackoffStrategies.exponentialWait(40, TimeUnit.MILLISECONDS);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(1, 0)) == 2);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(2, 0)) == 4);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(3, 0)) == 8);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(4, 0)) == 16);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(5, 0)) == 32);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(6, 0)) == 40);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(7, 0)) == 40);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(Integer.MAX_VALUE, 0)) == 40);
    }

    @Test
    public void testExponentialWithMultiplierAndMaximumWait() {
        BackoffStrategy exponentialWait = BackoffStrategies.exponentialWait(1000, 50000, TimeUnit.MILLISECONDS);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(1, 0)) == 2000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(2, 0)) == 4000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(3, 0)) == 8000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(4, 0)) == 16000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(5, 0)) == 32000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(6, 0)) == 50000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(7, 0)) == 50000);
        assertTrue(exponentialWait.computeBackoffMilliseconds(failedAttempt(Integer.MAX_VALUE, 0)) == 50000);
    }

    @Test
    public void testFibonacci() {
        BackoffStrategy fibonacciWait = BackoffStrategies.fibonacciWait();
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(1, 0L)) == 1L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(2, 0L)) == 1L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(3, 0L)) == 2L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(4, 0L)) == 3L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(5, 0L)) == 5L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(6, 0L)) == 8L);
    }

    @Test
    public void testFibonacciWithMaximumWait() {
        BackoffStrategy fibonacciWait = BackoffStrategies.fibonacciWait(10L, TimeUnit.MILLISECONDS);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(1, 0L)) == 1L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(2, 0L)) == 1L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(3, 0L)) == 2L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(4, 0L)) == 3L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(5, 0L)) == 5L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(6, 0L)) == 8L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(7, 0L)) == 10L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(Integer.MAX_VALUE, 0L)) == 10L);
    }

    @Test
    public void testFibonacciWithMultiplierAndMaximumWait() {
        BackoffStrategy fibonacciWait = BackoffStrategies.fibonacciWait(1000L, 50000L, TimeUnit.MILLISECONDS);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(1, 0L)) == 1000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(2, 0L)) == 1000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(3, 0L)) == 2000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(4, 0L)) == 3000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(5, 0L)) == 5000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(6, 0L)) == 8000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(7, 0L)) == 13000L);
        assertTrue(fibonacciWait.computeBackoffMilliseconds(failedAttempt(Integer.MAX_VALUE, 0L)) == 50000L);
    }

    @Test
    public void testExceptionWait() {
        BackoffStrategy exceptionWait = BackoffStrategies.exceptionWait(RuntimeException.class, zeroSleepFunction());
        assertEquals(0L, exceptionWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        BackoffStrategy oneMinuteWait = BackoffStrategies.exceptionWait(RuntimeException.class, oneMinuteSleepFunction());
        assertEquals(3600 * 1000L, oneMinuteWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        BackoffStrategy noMatchRetryAfterWait = BackoffStrategies.exceptionWait(RetryAfterException.class, customSleepFunction());
        assertEquals(0L, noMatchRetryAfterWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        BackoffStrategy retryAfterWait = BackoffStrategies.exceptionWait(RetryAfterException.class, customSleepFunction());
        assertEquals(29L, retryAfterWait.computeBackoffMilliseconds(failedRetryAfterAttempt(42, 7227)));
    }

    public Attempt<Void> failedAttempt(long attemptNumber, long delaySinceFirstAttempt) {
    	return Attempt.withException(null, attemptNumber, Instant.now().minusMillis(delaySinceFirstAttempt), Instant.now(), new RuntimeException());
    }

    public Attempt<Void> failedRetryAfterAttempt(long attemptNumber, long delaySinceFirstAttempt) {
    	return Attempt.withException(null, attemptNumber, Instant.now().minusMillis(delaySinceFirstAttempt), Instant.now(), new RetryAfterException());
    }

    public Function<RuntimeException, Long> zeroSleepFunction() {
        return new Function<RuntimeException, Long>() {
            @Override
            public Long apply(RuntimeException input) {
                return 0L;
            }
        };
    }

    public Function<RuntimeException, Long> oneMinuteSleepFunction() {
        return new Function<RuntimeException, Long>() {
            @Override
            public Long apply(RuntimeException input) {
                return 3600 * 1000L;
            }
        };
    }

    public Function<RetryAfterException, Long> customSleepFunction() {
        return new Function<RetryAfterException, Long>() {
            @Override
            public Long apply(RetryAfterException input) {
                return input.getRetryAfter();
            }
        };
    }

    public class RetryAfterException extends RuntimeException {
        private final long retryAfter = 29L;

        public long getRetryAfter() {
            return retryAfter;
        }
    }
}
