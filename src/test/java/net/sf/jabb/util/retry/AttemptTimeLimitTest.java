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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

/**
 * @author Jason Dunkelberger (dirkraft)
 * @author James Hu
 */
public class AttemptTimeLimitTest {

    AttemptStrategy r = new AttemptStrategy()
    		.withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
            .withAttemptTimeLimit(new SimpleTimeLimiter(), Duration.ofSeconds(1));
    
    
    @Test
    public void testAttemptTimeLimit() {
        SleepyOut.resetCounts();

        try {
            r.runThrowingAttempt(new SleepyOut(100L));
        } catch (Exception e) {
            Assert.fail("Should not timeout");
        }
        assertEquals(1, SleepyOut.finishedCount);
        assertEquals(0, SleepyOut.interruptedCount);

        SleepyOut.resetCounts();

        try {
        	r.runThrowingAttempt(new SleepyOut(10 * 1000L));
            Assert.fail("should have exception");
        } catch (UncheckedTimeoutException e) {
        	Assert.fail("should not have UncheckedTimeoutException");
        } catch(Exception e){
            Assert.assertEquals(TooManyAttemptsException.class, e.getClass());
            TooManyAttemptsException ex = (TooManyAttemptsException)e;
            assertNotNull(ex.getLastAttempt());
            assertEquals(3, ex.getLastAttempt().getTotalAttempts());
            assertTrue(ex.getLastAttempt().hasException());
            assertEquals(UncheckedTimeoutException.class, ex.getLastAttempt().getException().getClass());
        }
        assertEquals(0, SleepyOut.finishedCount);
        assertEquals(3, SleepyOut.interruptedCount);
    }

    static class SleepyOut implements Runnable {

        final long sleepMs;
        static int interruptedCount = 0;
        static int finishedCount = 0;
        
        static public void resetCounts(){
            SleepyOut.interruptedCount = 0;
            SleepyOut.finishedCount = 0;
        }

        SleepyOut(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void run(){
            try {
				Thread.sleep(sleepMs);
				finishedCount ++;
			} catch (InterruptedException e) {
				interruptedCount ++;
			}
        }
    }
}
