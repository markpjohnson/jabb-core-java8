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

/**
 * A strategy used to decide how long to backoff before attempts.
 *
 * @author JB
 * @author James Hu
 */
public interface BackoffStrategy {

    /**
     * Returns the time, in milliseconds, to wait before next attempt.
     *
     * @param attempt the previous {@code Attempt}
     * @return the sleep time before next attempt
     */
    <T> long computeBackoffMilliseconds(Attempt<T> attempt);
}
