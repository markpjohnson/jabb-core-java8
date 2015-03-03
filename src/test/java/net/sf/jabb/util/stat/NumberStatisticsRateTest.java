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
public class NumberStatisticsRateTest extends BaseTest {

	@Test
	public void testAtomicLongStatistics() throws Exception {
		NumberStatistics<Long> stat = new AtomicLongStatistics();
		doStatisticsTest(stat);
	}

	@Test
	public void testConcurrentLongStatistics() throws Exception {
		NumberStatistics<Long> stat = new ConcurrentLongStatistics();
		doStatisticsTest(stat);
	}

	@Test
	public void testConcurrentBigIntegerStatistics() throws Exception {
		NumberStatistics<BigInteger> stat = new ConcurrentBigIntegerStatistics();
		doStatisticsTest(stat);
	}
	
	@Test
	public void testConcurrentBigIntegerStatistics50() throws Exception {
		NumberStatistics<BigInteger> stat = new ConcurrentBigIntegerStatistics(50);
		doStatisticsTest(stat);
	}
	
	@Test
	public void testConcurrentBigIntegerStatistics1() throws Exception {
		NumberStatistics<BigInteger> stat = new ConcurrentBigIntegerStatistics(1);
		doStatisticsTest(stat);
	}
	
	protected void doStatisticsTest(NumberStatistics<?> stat) throws Exception{
		stat.reset();
		doStatisticsTestInt(stat.getClass().getSimpleName() + " - int", stat, randomIntegers);
		stat.reset();
		doStatisticsTestLong(stat.getClass().getSimpleName() + " - long", stat, randomLongs);
		stat.reset();
		doStatisticsTestBigInteger(stat.getClass().getSimpleName() + " - int as BigInteger", stat, randomIntegersAsBigIntegers);
		stat.reset();
		doStatisticsTestBigInteger(stat.getClass().getSimpleName() + " - long as BigInteger", stat, randomLongsAsBigIntegers);
		stat.reset();
		doStatisticsTestBigInteger(stat.getClass().getSimpleName() + " - BigInteger", stat, randomBigIntegers);
	}

	protected void doStatisticsTestInt(String title, NumberStatistics<?> stat, int[] randomIntegers) throws Exception {
		RateTestUtility.doRateTest(title, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomIntegers.length && System.currentTimeMillis() < endTime; i ++){
						stat.evaluate(randomIntegers[i]);
					}
					return i;
				});
	}
	
	protected void doStatisticsTestLong(String title, NumberStatistics<?> stat, long[] randomLongs) throws Exception {
		RateTestUtility.doRateTest(title, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomLongs.length && System.currentTimeMillis() < endTime; i ++){
						stat.evaluate(randomLongs[i]);
					}
					return i;
				});
	}
	
	protected void doStatisticsTestBigInteger(String title, NumberStatistics<?> stat, BigInteger[] randomBigIntegers) throws Exception {
		RateTestUtility.doRateTest(title, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomBigIntegers.length && System.currentTimeMillis() < endTime; i ++){
						stat.evaluate(randomBigIntegers[i]);
					}
					return i;
				});
	}
	



}
