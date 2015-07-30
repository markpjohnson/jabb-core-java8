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
 * A strategy used to decide whether further attempts are allowed or not.
 *
 * @author JB
 * @author James Hu
 */
public interface StopStrategy {

    /**
     * Returns <code>true</code> if the no more attempt is allowed.
     *
     * @param attempt the previous {@code Attempt}
     * @return <code>true</code> if should stop attempting, <code>false</code> otherwise
     */
    <T> boolean shouldStop(Attempt<T> attempt);
}
