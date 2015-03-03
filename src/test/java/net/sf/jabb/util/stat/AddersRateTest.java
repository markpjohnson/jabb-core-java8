/**
 * 
 */
package net.sf.jabb.util.stat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import net.sf.jabb.util.test.RateTestUtility;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AddersRateTest extends BaseTest{
	
	@Test
	public void testLongAdder() throws Exception{
		LongAdder longAdder = new LongAdder();
		doTestAdder(longAdder.getClass().getSimpleName(), longAdder);
	}
	
	@Test
	public void testBigIntegerAdder() throws Exception{
		BigIntegerAdder bigIntegerAdder = new BigIntegerAdder();
		doTestAdder(bigIntegerAdder.getClass().getSimpleName(), bigIntegerAdder);
	}

	
	protected void doTestAdder(String title, LongAdder adder) throws Exception{
		adder.reset();
		RateTestUtility.doRateTest(title + " - int", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomIntegers.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomIntegers[i]);
					}
					return i;
				});
		
		adder.reset();
		RateTestUtility.doRateTest(title + " - long", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomLongs.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomLongs[i]);
					}
					return i;
				});
	}
	
	protected void doTestAdder(String title, BigIntegerAdder adder) throws Exception{
		adder.reset();
		RateTestUtility.doRateTest(title + " - int", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomIntegers.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomIntegers[i]);
					}
					return i;
				});

		adder.reset();
		RateTestUtility.doRateTest(title + " - long", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomLongs.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomLongs[i]);
					}
					return i;
				});

		adder.reset();
		RateTestUtility.doRateTest(title + " - int as BigInteger", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomIntegersAsBigIntegers.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomIntegersAsBigIntegers[i]);
					}
					return i;
				});

		adder.reset();
		RateTestUtility.doRateTest(title + " - long as BigInteger", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomLongsAsBigIntegers.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomLongsAsBigIntegers[i]);
					}
					return i;
				});

		adder.reset();
		RateTestUtility.doRateTest(title + " - BigInteger", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomBigIntegers.length && System.currentTimeMillis() < endTime; i ++){
						adder.add(randomBigIntegers[i]);
					}
					return i;
				});
	}
	

}
