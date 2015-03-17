package net.sf.jabb.util.parallel;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.sf.jabb.util.col.AtomicComputeIfAbsentMap;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class BasicLoadBalancerTest {
	
	protected Map<Integer, AtomicLong> processedStatistics = new AtomicComputeIfAbsentMap<>(new HashMap<Integer, AtomicLong>(), AtomicLong::new);
	
	protected int buckets = 100000;
	protected List<Integer> processors = Arrays.asList(new Integer[]{1, 2, 3, 4});
	protected List<Integer> backupProcessors = Arrays.asList(new Integer[]{5, 6});
	protected int samples = buckets * 100;
	
	protected LoadBalancer<String, String, Integer> lb;
	
	protected float distributionCorrectnessThreshold = 0.25f;
	
	protected void createLoadBalancer() {
		this.lb = new BasicLoadBalancer<String, String, Integer>(buckets, processors, backupProcessors,
				null, s -> s.hashCode(), (processor, load) -> {
					return load;
				}, stat->{
					processedStatistics.get(stat.getProcessor()).incrementAndGet();
				}, null);
	}
	
	@Test
	public void testAddAndRemove(){
		createLoadBalancer();
		doTestEvenDistribution(1, 4);
		lb.remove(1);
		doTestEvenDistribution(2, 4);
		lb.remove(2);
		doTestEvenDistribution(3, 4);
		lb.remove(3);
		doTestEvenDistribution(4, 4);
		lb.add(3);
		doTestEvenDistribution(3, 4);
		lb.add(2);
		doTestEvenDistribution(2, 4);
		lb.add(1);
		doTestEvenDistribution(1, 4);
	}
	
	protected void dispatchSamples(){
		processedStatistics.clear();
		
		int numThreads = 10;
		Thread[] threads = new Thread[numThreads];
		for (int j = 0; j < threads.length; j ++){
			threads[j] = new Thread(){
				@Override
				public void run(){
					for (int i = 0; i < samples/numThreads; i ++){
						lb.dispatch(RandomStringUtils.random(10));
					}
				}
			};
			threads[j].start();
		}
		for (int j = 0; j < threads.length; j ++){
			Uninterruptibles.joinUninterruptibly(threads[j]);
		}
	}
	
	protected void doTestEvenDistribution(int processorStart, int processorEnd){
		dispatchSamples();
		int average = samples/(processorEnd - processorStart + 1);
		for (int i = processorStart; i <= processorEnd; i ++){
			assertAroundTarget(average, processedStatistics.get(i).get());
		}
	}
	
	protected void assertAroundTarget(long target, long actual){
		float difference = (float)Math.abs(actual - target)/target;
		assertTrue("Difference should be smaller than " + distributionCorrectnessThreshold + " but actually is " + difference, 
				difference < distributionCorrectnessThreshold);
	}
	
	@Test
	public void testAdjustLoad(){
		createLoadBalancer();
		doTestEvenDistribution(1, 4);		// 25, 25, 25, 25
		
		lb.increaseLoad(1, 0.15f);			// 40, 20, 20, 20
		dispatchSamples();
		assertAroundTarget((long)(samples*0.4f), processedStatistics.get(1).get());
		assertAroundTarget((long)(samples*0.2f), processedStatistics.get(2).get());
		assertAroundTarget((long)(samples*0.2f), processedStatistics.get(3).get());
		assertAroundTarget((long)(samples*0.2f), processedStatistics.get(4).get());

		lb.decreaseLoad(4, 0.15f);			// 45, 25, 25, 5
		dispatchSamples();
		assertAroundTarget((long)(samples*0.45f), processedStatistics.get(1).get());
		assertAroundTarget((long)(samples*0.25f), processedStatistics.get(2).get());
		assertAroundTarget((long)(samples*0.25f), processedStatistics.get(3).get());
		assertAroundTarget((long)(samples*0.05f), processedStatistics.get(4).get());
	
	}
	
}
