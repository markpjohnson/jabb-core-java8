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
 * This is a strategy used to decide how a specific time duration need to be waited.
 * Normally this is just a Thread.sleep(), but implementations can be
 * something more elaborate if desired.
 */
public interface WaitStrategy {

    /**
     * Wait for the designated amount of time. 
     *
     * @param timeInMilliseconds 		the duration in milliseconds
     * @throws InterruptedException		if got interrupted
     */
    void await(long timeInMilliseconds) throws InterruptedException;
}