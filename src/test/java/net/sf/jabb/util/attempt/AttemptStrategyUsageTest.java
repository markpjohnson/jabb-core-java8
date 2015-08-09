/**
 * 
 */
package net.sf.jabb.util.attempt;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import net.sf.jabb.util.parallel.BackoffStrategies;

import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

/**
 * @author James Hu
 *
 */
public class AttemptStrategyUsageTest {

	@Test
	public void testConstruction() throws Exception {
		AttemptStrategy strategyOne = new AttemptStrategy();
		AttemptStrategy strategyTwo = AttemptStrategy.create();
		
		AttemptStrategy attemptStrategy = new AttemptStrategy()
			.withBackoffStrategy(BackoffStrategies.fixedBackoff(1, TimeUnit.MINUTES))
			.withStopStrategy(StopStrategies.stopAfterTotalAttempts(10));
		performOperation1(attemptStrategy);
		Integer result = performOperation2(attemptStrategy);
	}
	
	@Test
	public void testEasyUsage() throws Exception{
		new AttemptStrategy()
		.withBackoffStrategy(BackoffStrategies.fixedBackoff(1, TimeUnit.MINUTES))
		.retryIfException(IllegalStateException.class)
		.retryIfException(StorageException.class, e->e.getHttpStatusCode() == 412)
		.run(()->operation1());
		
		Integer result = new AttemptStrategy()
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(1000L, 1, TimeUnit.MINUTES))
		.retryIfException(StorageException.class, e->e.getHttpStatusCode() == 412)
		.retryIfResultEquals(Integer.valueOf(-1))
		.call(()->operation2());

	}

	protected void performOperation1(AttemptStrategy attemptStrategy) throws Exception{
		new AttemptStrategy(attemptStrategy)
			.overrideBackoffStrategy(BackoffStrategies.noBackoff())
			.retryIfException(IllegalStateException.class)
			.run(()->operation1());
	}

	protected Integer performOperation2(AttemptStrategy attemptStrategy) throws Exception{
		return new AttemptStrategy(attemptStrategy)
			.retryIfResultEquals(Integer.valueOf(-1))
			.call(()->operation2());
	}

	private void operation1() {
	}
	private Integer operation2() {
		return Integer.valueOf(1);
	}
}
