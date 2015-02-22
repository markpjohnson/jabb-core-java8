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
public class AdderRateTest {
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
	public void testLongAdder() throws Exception {
		LongAdder adder = new LongAdder();
		RateTestUtility.doRateTest("LongAdder", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						adder.add(longTestValue1);
						adder.add(longTestValue2);
					}
					return i*2;
				});
	}

	@Test
	public void testBigIntegerAdder_int() throws Exception {
		BigIntegerAdder adder = new BigIntegerAdder();
		RateTestUtility.doRateTest("BigIntegerAdder - int", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						adder.add(intTestValue1);
						adder.add(intTestValue2);
					}
					return i*2;
				});
	}

	@Test
	public void testBigIntegerAdder_int_small() throws Exception {
		BigIntegerAdder adder = new BigIntegerAdder();
		RateTestUtility.doRateTest("BigIntegerAdder - small int", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						adder.add(2837468);
						adder.add(7324);
					}
					return i*2;
				});
	}

	@Test
	public void testBigIntegerAdder_small_big() throws Exception {
		BigIntegerAdder adder = new BigIntegerAdder();
		RateTestUtility.doRateTest("BigIntegerAdder - small BigInteger", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						adder.add(bitIntegerTestValue1);
						adder.add(bitIntegerTestValue2);
					}
					return i*2;
				});
	}

	@Test
	public void testBigIntegerAdder_big_big() throws Exception {
		BigIntegerAdder adder = new BigIntegerAdder();
		RateTestUtility.doRateTest("BigIntegerAdder - big BigInteger", testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, null, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < batchSize && System.currentTimeMillis() < endTime; i ++){
						adder.add(bitIntegerTestValue3);
						adder.add(bitIntegerTestValue4);
					}
					return i*2;
				});
	}


}
