/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import net.sf.jabb.util.test.RateTestUtility;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class MinMaxHoldersRateTest extends BaseTest{
	
	@Test
	public void testConcurrentLongMinMaxHolder() throws Exception{
		MinMaxHolder holder = new ConcurrentLongMinMaxHolder();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - long", holder, randomLongs);
		holder.reset();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - int as BigInteger", holder, randomIntegersAsBigIntegers);
		holder.reset();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - long as BigInteger", holder, randomLongsAsBigIntegers);
	}
	
	@Test
	public void testConcurrentBigIntegerMinMaxHolder() throws Exception{
		MinMaxHolder holder = new ConcurrentBigIntegerMinMaxHolder();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - long", holder, randomLongs);
		holder.reset();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - int as BigInteger", holder, randomIntegersAsBigIntegers);
		holder.reset();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - long as BigInteger", holder, randomLongsAsBigIntegers);
		holder.reset();
		doMinMaxHolderTest(holder.getClass().getSimpleName() + " - BigInteger", holder, randomBigIntegers);
	}

	
	protected void doMinMaxHolderTest(String title, MinMaxHolder holder, long[] randomLongs) throws Exception {
		RateTestUtility.doRateTest(title, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomLongs.length && System.currentTimeMillis() < endTime; i ++){
						holder.evaluate(randomLongs[i]);
					}
					return i;
				});
	}

	protected void doMinMaxHolderTest(String title, MinMaxHolder holder, BigInteger[] randomBigIntegers) throws Exception {
		RateTestUtility.doRateTest(title, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomBigIntegers.length && System.currentTimeMillis() < endTime; i ++){
						holder.evaluate(randomBigIntegers[i]);
					}
					return i;
				});
	}


}
