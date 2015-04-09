/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jabb.util.col.AtomicComputeIfAbsentMap;
import net.sf.jabb.util.col.ComputeIfAbsentConcurrentHashMap;
import net.sf.jabb.util.stat.RotatableNumberStatisticsMap.RotatedMap;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mapdb.DBMaker;


/**
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RotatableNumberStatisticsMapTest {

	@Test
	public void test1Creation() {
		RotatableNumberStatisticsMap<String, BigInteger, ConcurrentBigIntegerStatistics> bigMap = new RotatableNumberStatisticsMap<>(()->{
			return new ComputeIfAbsentConcurrentHashMap<>(k->{return new ConcurrentBigIntegerStatistics();});
		});
		assertEquals(1, bigMap.all.size());
		bigMap.evaluate("string1", BigInteger.ONE);
		bigMap.evaluate("string1", BigInteger.TEN);
		bigMap.evaluate("string2", 2);
		assertEquals(2, bigMap.current.map.size());
		
		@SuppressWarnings("rawtypes")
		RotatedMap previousCurrent = bigMap.current;
		
		bigMap.rotate();
		assertEquals(2, bigMap.all.size());
		bigMap.evaluate("string2", 2);
		bigMap.evaluate("string3", 3);
		assertEquals(2, bigMap.current.map.size());
		assertEquals(previousCurrent, bigMap.all.peek());
		
		NumberStatistics<BigInteger> statisticsBig = bigMap.getStatistics("string2", null);
		assertEquals(2, statisticsBig.getCount());
		assertEquals(BigInteger.valueOf(4), statisticsBig.getSum());
	
		RotatableNumberStatisticsMap<String, Long, ConcurrentLongStatistics> longMap = new RotatableNumberStatisticsMap<>(()->{
			return new AtomicComputeIfAbsentMap<>(new HashMap<>(), k->{return new ConcurrentLongStatistics();});
		});
		longMap.evaluate("string1", BigInteger.ONE);
		longMap.evaluate("string1", BigInteger.TEN);
		longMap.evaluate("string2", 2);
		longMap.rotate();
		longMap.evaluate("string2", 2);
		longMap.evaluate("string3", 3);
		
		NumberStatistics<BigInteger> statisticsLong = longMap.getStatistics("string2", null);
		assertEquals(2, statisticsLong.getCount());
		assertEquals(BigInteger.valueOf(4), statisticsLong.getSum());
		
		assertEquals(2, longMap.all.size());
		longMap.purge(null, System.currentTimeMillis()+1);
		assertEquals(1, longMap.all.size());
		longMap.rotate();
		longMap.purge(null, System.currentTimeMillis()+1);
		assertEquals(1, longMap.all.size());
	}
	
	@Test
	public void test2SwapAndPurge() throws InterruptedException{
		RotatableNumberStatisticsMap<String, BigInteger, CustomStatistics> bigMap = new RotatableNumberStatisticsMap<>(()->{
			return new ComputeIfAbsentConcurrentHashMap<>(k->{return new CustomStatistics();});
		});

		long start = System.currentTimeMillis();
		for (int i = 0; i < 120; i ++){				// rotate every half second for 1 minute
			bigMap.evaluate("string1", 10000L+i);
			bigMap.evaluate("string1", 20000L+i);
			bigMap.evaluate("string2", 30000L+i);
			bigMap.evaluate("string2", 40000L+i);
			Thread.sleep(500L);
			bigMap.rotate();
		}
		assertEquals(121, bigMap.all.size());
		
		bigMap.swap(original->{
			Map<String, CustomStatistics> newMap = DBMaker.newTempHashMap();
			newMap.putAll(original);
			return newMap;
		}, 1, start + 1000L*30+100);
		assertEquals(121, bigMap.all.size());
		
		NumberStatistics<BigInteger> stat = bigMap.getStatistics("string1", null);
		assertEquals(240L, stat.getCount());
		
		stat = bigMap.getStatistics("string1", s-> !s.shouldIgnore);
		assertEquals(240L, stat.getCount());
		
		List<Map<String, CustomStatistics>> list = bigMap.getRotatedMaps(1, start + 1000L*40+100);
		assertEquals(80, list.size());		// 2/3 of 120
		
		list.stream().forEach(m ->{
			m.values().forEach(s-> s.shouldIgnore = true);
		});
		stat = bigMap.getStatistics("string1", s-> !s.shouldIgnore);
		assertEquals(80L, stat.getCount());		// 1/3 of 240
		
		bigMap.purge(null, start + 1000L*20+100);
		assertEquals(81, bigMap.all.size());
	}

	static class CustomStatistics extends ConcurrentBigIntegerStatistics{
		private static final long serialVersionUID = -7340748155748858767L;
		boolean shouldIgnore = false;
	}
}
