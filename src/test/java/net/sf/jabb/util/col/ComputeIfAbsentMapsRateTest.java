/**
 * 
 */
package net.sf.jabb.util.col;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import net.sf.jabb.util.stat.NumberGenerator;
import net.sf.jabb.util.test.RateTestUtility;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class ComputeIfAbsentMapsRateTest {
	protected int warmUpSeconds = 2;
	protected int testSeconds = 10;
	protected int testThreads = 50;
	protected int batchSize = 1000;
	protected int lengthOfRandomNumbers = batchSize * 1000;

	protected int[] randomIntegers = NumberGenerator.randomIntegers(0, Integer.MAX_VALUE/10, lengthOfRandomNumbers);

	@Test
	public void test() throws Exception {
		Function<Integer, LongAdder> computeFunction = k -> new LongAdder();
		MapValueFactory<Integer, LongAdder> valueFactory = new MapValueFactory<Integer, LongAdder>(){
			@Override
			public LongAdder createValue(Integer key) {
				return new LongAdder();
			}
		};
		ExecutorService threadPool = Executors.newFixedThreadPool(testThreads);
		
		Map<String, Map<Integer, LongAdder>> maps = new LinkedHashMap<>();
		maps.put("PutIfAbsentMap<HashMap> - Reflection", new PutIfAbsentMap<Integer, LongAdder>(new HashMap<Integer, LongAdder>(), LongAdder.class));	
		maps.put("PutIfAbsentMap<ConcurrentHashMap> - Reflection", new PutIfAbsentMap<Integer, LongAdder>(new ConcurrentHashMap<Integer, LongAdder>(), LongAdder.class));	
		
		maps.put("PutIfAbsentMap<HashMap> - Factory", new PutIfAbsentMap<Integer, LongAdder>(new HashMap<Integer, LongAdder>(), valueFactory));	
		maps.put("PutIfAbsentMap<ConcurrentHashMap> - Factory", new PutIfAbsentMap<Integer, LongAdder>(new ConcurrentHashMap<Integer, LongAdder>(), valueFactory));	
		
		maps.put("ComputeIfAbsentConcurrentHashMap", new ComputeIfAbsentConcurrentHashMap<Integer, LongAdder>(computeFunction));	
		maps.put("ComputeIfAbsentConcurrentSkipListMap", new ComputeIfAbsentConcurrentSkipListMap<Integer, LongAdder>(computeFunction));	
		
		maps.put("AtomicComputeIfAbsentMap<HashMap>", new AtomicComputeIfAbsentMap<Map<Integer, LongAdder>, Integer, LongAdder>(new HashMap<Integer, LongAdder>(), computeFunction));	
		maps.put("AtomicComputeIfAbsentMap<ConcurrentHashMap>", new AtomicComputeIfAbsentMap<Map<Integer, LongAdder>, Integer, LongAdder>(new ConcurrentHashMap<Integer, LongAdder>(), computeFunction));	

		
		for (int i = 0; i < 5; i ++){
			System.out.println("================");
			for (Map.Entry<String, Map<Integer, LongAdder>> entry: maps.entrySet()){
				doTests(entry.getKey(), entry.getValue(), threadPool);
				entry.getValue().clear();
			}
		}
	}
	
	protected void doTests(String title, Map<Integer, LongAdder> map, ExecutorService threadPool) throws Exception{
		System.out.println("-----");
		for (int testSeconds: new int[] {1, 2, 5, 10, 15, 30, 60}){
			doTest(title, map, threadPool, testSeconds);
		}
	}
	
	protected void doTest(String title, Map<Integer, LongAdder> map, ExecutorService threadPool, int testSeconds) throws Exception{
		map.clear();
		RateTestUtility.doRateTest(title + " - " + testSeconds, threadPool, testThreads, 
				warmUpSeconds, TimeUnit.SECONDS, RateTestUtility.emptyLoop, 
				testSeconds, TimeUnit.SECONDS, endTime -> {
					int i;
					for (i = 0; i < randomIntegers.length && System.currentTimeMillis() < endTime; i ++){
						map.get(i).increment();
					}
					return i;
				});
	}
	


}
