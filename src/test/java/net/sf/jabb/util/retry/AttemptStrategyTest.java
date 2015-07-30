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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategy;

import org.junit.Test;

public class AttemptStrategyTest {

    @Test
    public void testWithBackoffStrategy() {
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withBackoffStrategy(BackoffStrategies.fixedBackoff(50L, TimeUnit.MILLISECONDS))
                .retryIfResult(x-> x == null);
        long start = System.currentTimeMillis();
        boolean result = retryer.call(callable);
        assertTrue(System.currentTimeMillis() - start >= 250L);
        assertTrue(result);
    }

    @Test
    public void testWithMoreThanOneBackoffStrategyOneBeingFixed() {
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withBackoffStrategy(BackoffStrategies.join(
                        BackoffStrategies.fixedBackoff(50L, TimeUnit.MILLISECONDS),
                        BackoffStrategies.fibonacciBackoff(10, Long.MAX_VALUE, TimeUnit.MILLISECONDS)))
                .retryIfResult(x-> x == null);
        long start = System.currentTimeMillis();
        boolean result = retryer.call(callable);
        assertTrue(System.currentTimeMillis() - start >= 370L);
        assertTrue(result);
    }

    @Test
    public void testWithMoreThanOneBackoffStrategyOneBeingIncremental(){
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withBackoffStrategy(BackoffStrategies.join(
                        BackoffStrategies.linearBackoff(10L, TimeUnit.MILLISECONDS, 10L, TimeUnit.MILLISECONDS),
                        BackoffStrategies.fibonacciBackoff(10, Long.MAX_VALUE, TimeUnit.MILLISECONDS)))
                .retryIfResult(x-> x == null);
        long start = System.currentTimeMillis();
        boolean result = retryer.call(callable);
        assertTrue(System.currentTimeMillis() - start >= 270L);
        assertTrue(result);
    }

    private Callable<Boolean> notNullAfter5Attempts() {
        return new Callable<Boolean>() {
            int counter = 0;

            @Override
            public Boolean call() throws Exception {
                if (counter < 5) {
                    counter++;
                    return null;
                }
                return true;
            }
        };
    }

    @Test
    public void testWithStopStrategy() throws InterruptedBeforeAttemptException, Exception  {
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .retryIfResult(x-> x == null);
        try {
            retryer.callThrowingAll(callable);
            fail("RetryException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
        }
    }

    @Test
    public void testWithWaitStrategy() {
        Callable<Boolean> callable = notNullAfter5Attempts();
        final AtomicInteger counter = new AtomicInteger();
        WaitStrategy blockStrategy = new WaitStrategy() {
            @Override
            public void await(long sleepTime) throws InterruptedException {
                counter.incrementAndGet();
            }

			@Override
			public void handleInterruptedException(InterruptedException e) {}
        };

        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withWaitStrategy(blockStrategy)
                .retryIfResult(x-> x == null);
        final int retryCount = 5;
        boolean result = retryer.call(callable);
        assertTrue(result);
        assertEquals(counter.get(), retryCount);
    }

    @Test
    public void testRetryIfException() throws InterruptedBeforeAttemptException, Exception {
        Callable<Boolean> callable = noIOExceptionAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfException()
                .toCallableAttemptStrategy();
        boolean result = retryer.call(callable);
        assertTrue(result);

        callable = noIOExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfException()
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("RetryException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IOException);
            assertTrue(e.getCause() instanceof IOException);
        }

        callable = noIllegalStateExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfException()
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("RetryException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IllegalStateException);
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    private Callable<Boolean> noIllegalStateExceptionAfter5Attempts() {
        return new Callable<Boolean>() {
            int counter = 0;

            @Override
            public Boolean call() throws Exception {
                if (counter < 5) {
                    counter++;
                    throw new IllegalStateException();
                }
                return true;
            }
        };
    }

    private Callable<Boolean> noIOExceptionAfter5Attempts() {
        return new Callable<Boolean>() {
            int counter = 0;

            @Override
            public Boolean call() throws IOException {
                if (counter < 5) {
                    counter++;
                    throw new IOException();
                }
                return true;
            }
        };
    }

    @Test
    public void testRetryIfRuntimeException() throws InterruptedBeforeAttemptException, Exception  {
        Callable<Boolean> callable = noIOExceptionAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfRuntimeException()
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("IOException expected");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
        }

        callable = noIllegalStateExceptionAfter5Attempts();
        assertTrue(retryer.call(callable));

        callable = noIllegalStateExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfRuntimeException()
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("TooManyAttemptsException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IllegalStateException);
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testRetryIfExceptionOfType() throws InterruptedBeforeAttemptException, Exception  {
        Callable<Boolean> callable = noIOExceptionAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfException(IOException.class)
                .toCallableAttemptStrategy();
        assertTrue(retryer.call(callable));

        callable = noIllegalStateExceptionAfter5Attempts();
        try {
            retryer.callThrowingAll(callable);
            fail("IllegalStateException expected");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }

        callable = noIOExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfException(IOException.class)
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("TooManyAttemptsException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IOException);
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testRetryIfExceptionWithPredicate() throws InterruptedBeforeAttemptException, Exception {
        Callable<Boolean> callable = noIOExceptionAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfException(new Predicate<Exception>() {
                    @Override
                    public boolean test(Exception t) {
                        return t instanceof IOException;
                    }
                })
                .toCallableAttemptStrategy();
        assertTrue(retryer.call(callable));

        callable = noIllegalStateExceptionAfter5Attempts();
        try {
            retryer.call(callable);
            fail("IllegalStateException expected");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }

        callable = noIOExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfException(new Predicate<Exception>() {
                    @Override
                    public boolean test(Exception t) {
                        return t instanceof IOException;
                    }
                })
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                .toCallableAttemptStrategy();
        try {
            retryer.callThrowingAll(callable);
            fail("TooManyAttemptsException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IOException);
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testRetryIfResult() throws InterruptedBeforeAttemptException, Exception  {
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfResult(x-> x == null)
                ;
        assertTrue(retryer.call(callable));

        callable = notNullAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfResult((Boolean x)-> x == null)
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                ;
        try {
            retryer.callThrowingAll(callable);
            fail("TooManyAttemptsException expected");
        } catch (TooManyAttemptsException e) {
            assertEquals(3, e.getLastAttempt().getTotalAttempts());
            assertFalse(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().hasResult());
            assertNull(e.getLastAttempt().getResult());
            assertNull(e.getCause());
        }
    }
    @Test
    public void testMultipleRetryConditions() throws InterruptedBeforeAttemptException, Exception {
        Callable<Boolean> callable = notNullResultOrIOExceptionOrRuntimeExceptionAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfResult((Boolean x)-> x == null)
                .retryIfException(IOException.class)
                .retryIfRuntimeException()
                .withStopStrategy(StopStrategies.stopAfterTotalAttempts(3))
                ;
        try {
            retryer.callThrowingAll(callable);
            fail("TooManyAttemptsException expected");
        } catch (TooManyAttemptsException e) {
            assertTrue(e.getLastAttempt().hasException());
            assertTrue(e.getLastAttempt().getException() instanceof IllegalStateException);
            assertTrue(e.getCause() instanceof IllegalStateException);
        }

        callable = notNullResultOrIOExceptionOrRuntimeExceptionAfter5Attempts();
        retryer = new AttemptStrategy()
                .retryIfResult((Boolean x)-> x == null)
                .retryIfException(IOException.class)
                .retryIfRuntimeException()
                ;
        assertTrue(retryer.call(callable));
    }

    private Callable<Boolean> notNullResultOrIOExceptionOrRuntimeExceptionAfter5Attempts() {
        return new Callable<Boolean>() {
            int counter = 0;

            @Override
            public Boolean call() throws IOException {
                if (counter < 1) {
                    counter++;
                    return null;
                } else if (counter < 2) {
                    counter++;
                    throw new IOException();
                } else if (counter < 5) {
                    counter++;
                    throw new IllegalStateException();
                }
                return true;
            }
        };
    }

    @Test
    public void testInterruption() throws InterruptedException {
        final AtomicBoolean result = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable r = new Runnable() {
            @Override
            public void run() {
                CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                        .withBackoffStrategy(BackoffStrategies.fixedBackoff(1000L, TimeUnit.MILLISECONDS))
                        .retryIfResult(x-> x == null)
                        ;
                try {
                    retryer.callThrowingAll(alwaysNull(latch));
                    fail("TooManyAttemptsException expected");
                } catch (InterruptedBeforeAttemptException e) {
                    assertFalse(e.getLastAttempt().hasException());
                    assertTrue(null != e.getCause());
                    assertTrue(Thread.currentThread().isInterrupted());
                    result.set(true);
				} catch (Exception e) {
                    fail("RetryException expected");
				}
            }
        };
        Thread t = new Thread(r);
        t.start();
        latch.countDown();
        t.interrupt();
        t.join();
        assertTrue(result.get());
    }

    /*
    @Test
    public void testWrap() throws ExecutionException, RetryException {
        Callable<Boolean> callable = notNullAfter5Attempts();
        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfResult(x-> x == null)
                ;
        RetryerCallable<Boolean> wrapped = retryer.wrap(callable);
        assertTrue(wrapped.call());
    }
    */

    @Test
    public void testWhetherBuilderFailsForNullStopStrategy() {
        try {
            new AttemptStrategy()
                    .withStopStrategy(null)
                    ;
            fail("Exepcted to fail for null stop strategy");
        } catch (NullPointerException exception) {
            assertTrue(exception.getMessage().contains("stopStrategy may not be null"));
        }
    }

    @Test
    public void testWhetherBuilderFailsForNullWaitStrategy() {
        try {
            new AttemptStrategy()
                    .withWaitStrategy(null)
                    ;
            fail("Exepcted to fail for null wait strategy");
        } catch (NullPointerException exception) {
            assertTrue(exception.getMessage().contains("waitStrategy may not be null"));
        }
    }

    @Test
    public void testWhetherBuilderFailsForNullWaitStrategyWithCompositeStrategies() {
        try {
            new AttemptStrategy()
                    .withBackoffStrategy(AttemptBackoffStrategies.join(null, null))
                    ;
            fail("Exepcted to fail for null wait strategy");
        } catch (IllegalStateException exception) {
            assertTrue(exception.getMessage().contains("Cannot have a null backoff strategy"));
        }
    }

    @Test
    public void testRetryListener_SuccessfulAttempt() throws Exception {
        final Map<Integer, Attempt> attempts = new HashMap<>();

        AttemptListener listener = new AttemptListener() {
            @Override
            public <V> void onAttempted(Attempt<V> attempt) {
                attempts.put(attempt.getTotalAttempts(), attempt);
            }
        };

        Callable<Boolean> callable = notNullAfter5Attempts();

        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfResult((Boolean x)-> x == null)
                .withAttemptListener(listener)
                ;
        assertTrue(retryer.call(callable));

        assertEquals(6, attempts.size());

        assertResultAttempt(attempts.get(1), true, null);
        assertResultAttempt(attempts.get(2), true, null);
        assertResultAttempt(attempts.get(3), true, null);
        assertResultAttempt(attempts.get(4), true, null);
        assertResultAttempt(attempts.get(5), true, null);
        assertResultAttempt(attempts.get(6), true, true);
    }

    @Test
    public void testRetryListener_WithException() throws Exception {
        final Map<Integer, Attempt> attempts = new HashMap<>();

        AttemptListener listener = new AttemptListener() {
            @Override
            public <V> void onAttempted(Attempt<V> attempt) {
                attempts.put(attempt.getTotalAttempts(), attempt);
            }
        };

        Callable<Boolean> callable = noIOExceptionAfter5Attempts();

        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .retryIfResult((Boolean x)-> x == null)
                .retryIfException()
                .withAttemptListener(listener)
                ;
        assertTrue(retryer.call(callable));

        assertEquals(6, attempts.size());

        assertExceptionAttempt(attempts.get(1), true, IOException.class);
        assertExceptionAttempt(attempts.get(2), true, IOException.class);
        assertExceptionAttempt(attempts.get(3), true, IOException.class);
        assertExceptionAttempt(attempts.get(4), true, IOException.class);
        assertExceptionAttempt(attempts.get(5), true, IOException.class);
        assertResultAttempt(attempts.get(6), true, true);
    }

    @Test
    public void testMultipleRetryListeners() throws Exception {
        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        };

        final AtomicBoolean listenerOne = new AtomicBoolean(false);
        final AtomicBoolean listenerTwo = new AtomicBoolean(false);

        CallableAttemptStrategy<Boolean> retryer = new AttemptStrategy()
                .withAttemptListener(new AttemptListener() {
                    @Override
                    public <V> void onAttempted(Attempt<V> attempt) {
                        listenerOne.set(true);
                    }
                })
                .withAttemptListener(new AttemptListener() {
                    @Override
                    public <V> void onAttempted(Attempt<V> attempt) {
                        listenerTwo.set(true);
                    }
                })
                .toCallableAttemptStrategy();

        assertTrue(retryer.call(callable));
        assertTrue(listenerOne.get());
        assertTrue(listenerTwo.get());
    }

    private void assertResultAttempt(Attempt actualAttempt, boolean expectedHasResult, Object expectedResult) {
        assertFalse(actualAttempt.hasException());
        assertEquals(expectedHasResult, actualAttempt.hasResult());
        assertEquals(expectedResult, actualAttempt.getResult());
    }

    private void assertExceptionAttempt(Attempt actualAttempt, boolean expectedHasException, Class<?> expectedExceptionClass) {
        assertFalse(actualAttempt.hasResult());
        assertEquals(expectedHasException, actualAttempt.hasException());
        assertTrue(expectedExceptionClass.isInstance(actualAttempt.getException()));
    }

    private Callable<Boolean> alwaysNull(final CountDownLatch latch) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                latch.countDown();
                return null;
            }
        };
    }
}
