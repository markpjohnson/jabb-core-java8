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

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

/**
 * Factory class for {@link StopStrategy} instances.
 *
 * @author JB
 * @author James Hu
 */
public final class StopStrategies {
    private static final StopStrategy NEVER_STOP = new NeverStopStrategy();

    private StopStrategies() {
    }

    /**
     * Returns a stop strategy which never stops retrying. It might be best to
     * try not to abuse services with this kind of behavior when small wait
     * intervals between retry attempts are being used.
     *
     * @return a stop strategy which never stops
     */
    public static StopStrategy neverStop() {
        return NEVER_STOP;
    }

    /**
     * Returns a stop strategy which stops after N failed attempts.
     *
     * @param maxAttemptNumber the number of failed attempts before stopping
     * @return a stop strategy which stops after {@code maxAttemptNumber} attempts
     */
    public static StopStrategy stopAfterTotalAttempts(int maxAttemptNumber) {
        return new StopAfterTotalAttemptsStrategy(maxAttemptNumber);
    }

    /**
     * Returns a stop strategy which stops after a given duration. If an
     * unsuccessful attempt is made, this {@link StopStrategy} will check if the
     * amount of time that's passed from the first attempt has exceeded the
     * given duration amount. If it has exceeded this duration, then using this
     * strategy causes the retrying to stop.
     *
     * @param duration the delay, starting from first attempt
     * @return a stop strategy which stops after {@code duration} 
     */
    public static StopStrategy stopAfterTotalDuration(Duration duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null");
        return new StopAfterTotalDurationStrategy(duration);
    }

    @Immutable
    private static final class NeverStopStrategy implements StopStrategy {
        @Override
        public <T> boolean shouldStop(Attempt<T> lastAttempt) {
            return false;
        }
    }

    @Immutable
    private static final class StopAfterTotalAttemptsStrategy implements StopStrategy {
        private final int maxAttemptNumber;

        public StopAfterTotalAttemptsStrategy(int maxAttemptNumber) {
            Preconditions.checkArgument(maxAttemptNumber >= 1, "maxAttemptNumber must be >= 1 but is %d", maxAttemptNumber);
            this.maxAttemptNumber = maxAttemptNumber;
        }

        @Override
        public <T> boolean shouldStop(Attempt<T> lastAttempt) {
            return lastAttempt.getTotalAttempts() >= maxAttemptNumber;
        }
    }

    @Immutable
    private static final class StopAfterTotalDurationStrategy implements StopStrategy {
        private final Duration duration;

        public StopAfterTotalDurationStrategy(Duration duration) {
        	Preconditions.checkNotNull(duration, "Duration cannot be null");
            Preconditions.checkArgument(!duration.isNegative() && !duration.isZero(), "Duration must be >= 0 but is %s", duration.toString());
            this.duration = duration;
        }

        @Override
        public <T> boolean shouldStop(Attempt<T> lastAttempt) {
            return Duration.between(lastAttempt.getFirstAttemptStartTime(), lastAttempt.getLastAttemptFinishTime()).compareTo(duration) >= 0;
        }
    }
}
