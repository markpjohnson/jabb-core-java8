/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.HashMap;

import net.sf.jabb.util.col.AtomicComputeIfAbsentMap;
import net.sf.jabb.util.col.ComputeIfAbsentConcurrentHashMap;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class RotatableNumberStatisticsMapTest {

	@Test
	public void testCreation() {
		RotatableNumberStatisticsMap<String, BigInteger> bigMap = new RotatableNumberStatisticsMap<String, BigInteger>(()->{
			return new ComputeIfAbsentConcurrentHashMap<>(k->{return new ConcurrentBigIntegerStatistics();});
		});
		bigMap.evaluate("string1", BigInteger.ONE);
		bigMap.evaluate("string1", BigInteger.TEN);
		bigMap.evaluate("string2", 2);
		bigMap.rotate();
		bigMap.evaluate("string2", 2);
		bigMap.evaluate("string3", 3);
		NumberStatistics<BigInteger> statisticsBig = bigMap.getOverallStatistics("string2");
	
		RotatableNumberStatisticsMap<String, Long> longMap = new RotatableNumberStatisticsMap<String, Long>(()->{
			return new AtomicComputeIfAbsentMap<>(new HashMap<>(), k->{return new ConcurrentLongStatistics();});
		});
		longMap.evaluate("string1", BigInteger.ONE);
		longMap.evaluate("string1", BigInteger.TEN);
		longMap.evaluate("string2", 2);
		longMap.rotate();
		longMap.evaluate("string2", 2);
		longMap.evaluate("string3", 3);
		NumberStatistics<BigInteger> statisticsLong = longMap.getOverallStatistics("string2");
		
		longMap.purgeIfRotatedBefore(System.currentTimeMillis());
	}

}
