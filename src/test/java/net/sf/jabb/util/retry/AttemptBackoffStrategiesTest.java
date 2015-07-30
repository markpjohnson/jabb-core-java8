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

public class AttemptBackoffStrategiesTest {



    @Test
    public void testExceptionWait() {
        AttemptBackoffStrategy exceptionWait = AttemptBackoffStrategies.exceptionBasedBackoff(RuntimeException.class, zeroSleepFunction());
        assertEquals(0L, exceptionWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        AttemptBackoffStrategy oneMinuteWait = AttemptBackoffStrategies.exceptionBasedBackoff(RuntimeException.class, oneMinuteSleepFunction());
        assertEquals(3600 * 1000L, oneMinuteWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        AttemptBackoffStrategy noMatchRetryAfterWait = AttemptBackoffStrategies.exceptionBasedBackoff(RetryAfterException.class, customSleepFunction());
        assertEquals(0L, noMatchRetryAfterWait.computeBackoffMilliseconds(failedAttempt(42, 7227)));

        AttemptBackoffStrategy retryAfterWait = AttemptBackoffStrategies.exceptionBasedBackoff(RetryAfterException.class, customSleepFunction());
        assertEquals(29L, retryAfterWait.computeBackoffMilliseconds(failedRetryAfterAttempt(42, 7227)));
    }

    public Attempt<Void> failedAttempt(int attemptNumber, long delaySinceFirstAttempt) {
    	return Attempt.withException(null, attemptNumber, Instant.now().minusMillis(delaySinceFirstAttempt), Instant.now(), new RuntimeException());
    }

    public Attempt<Void> failedRetryAfterAttempt(int attemptNumber, long delaySinceFirstAttempt) {
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
