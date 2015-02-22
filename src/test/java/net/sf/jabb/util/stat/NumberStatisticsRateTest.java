/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import net.sf.jabb.util.test.RateTestUtility;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class NumberStatisticsRateTest {
	static final int warmUpSeconds = 2;
	static final int testSeconds = 10;
	static final int testThreads = 50;
	static final int batchSize = 1000;
	
	static final long longTestValue1 = Integer.MAX_VALUE;
	static final long longTestValue2 = 99783;
	static final long longTestValue3 = Integer.MAX_VALUE /1000;
	static final long longTestValue4 = Integer.MAX_VALUE * 100;
	static final int intTestValue1 = Integer.MAX_VALUE/100;
	static final int intTestValue2 = 99783;
	static final BigInteger bitIntegerTestValue1 = BigInteger.valueOf(Integer.MAX_VALUE);
	static final BigInteger bitIntegerTestValue2= BigInteger.valueOf(99783);
	
	static final BigInteger bitIntegerTestValue3= BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE));
	static final BigInteger bitIntegerTestValue4= bitIntegerTestValue3.multiply(bitIntegerTestValue3);

	@Test
	public void testBasic_int_small() throws Exception {
		BasicNumberStatistics stat = new BasicNumberStatistics();
		RateTestUtility.doRateTest("BasicNumberStatistics", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						stat.put(83747);
						stat.put(98237434);
					}
					return i*2;
				});
	}

	@Test
	public void testAdvanced_int_small() throws Exception {
		AdvancedNumberStatistics stat = new AdvancedNumberStatistics();
		RateTestUtility.doRateTest("AdvancedNumberStatistics", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						stat.put(83747);
						stat.put(98237434);
					}
					return i*2;
				});
	}

}
