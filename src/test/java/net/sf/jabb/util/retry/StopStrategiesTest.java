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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

public class StopStrategiesTest {

    @Test
    public void testNeverStop() {
        assertFalse(StopStrategies.neverStop().shouldStop(failedAttempt(3, 6546L)));
    }

    @Test
    public void testStopAfterAttempt() {
        assertFalse(StopStrategies.stopAfterTotalAttempts(3).shouldStop(failedAttempt(2, 6546L)));
        assertTrue(StopStrategies.stopAfterTotalAttempts(3).shouldStop(failedAttempt(3, 6546L)));
        assertTrue(StopStrategies.stopAfterTotalAttempts(3).shouldStop(failedAttempt(4, 6546L)));
    }

    @Test
    public void testStopAfterDelayWithMilliseconds() {
        assertFalse(StopStrategies.stopAfterTotalDuration(Duration.ofMillis(1000L)).shouldStop(failedAttempt(2, 999L)));
        assertTrue(StopStrategies.stopAfterTotalDuration(Duration.ofMillis(1000L)).shouldStop(failedAttempt(2, 1000L)));
        assertTrue(StopStrategies.stopAfterTotalDuration(Duration.ofMillis(1000L)).shouldStop(failedAttempt(2, 1001L)));
    }

    @Test
    public void testStopAfterDelayWithTimeUnit() {
        assertFalse(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(1)).shouldStop(failedAttempt(2, 900L)));
        assertTrue(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(1)).shouldStop(failedAttempt(2, 1000L)));
        assertTrue(StopStrategies.stopAfterTotalDuration(Duration.ofSeconds(1)).shouldStop(failedAttempt(2, 1001L)));
    }

    public Attempt<Void> failedAttempt(int attemptNumber, long delaySinceFirstAttempt) {
    	return Attempt.withException(null, attemptNumber, Instant.now().minusMillis(delaySinceFirstAttempt), Instant.now(), new RuntimeException());
    }
}
