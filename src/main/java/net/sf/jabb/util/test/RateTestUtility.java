/**
 * 
 */
package net.sf.jabb.util.test;

import static org.junit.Assert.*;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author James Hu
 *
 */
public class RateTestUtility {
	
	public static LongConsumer emptyLoop = endTime -> {
		while(System.currentTimeMillis() < endTime){}
	};

	public static void doRateTest(ExecutorService threadPool, int numThreads, 
			int warmUpPeriod, TimeUnit warmUpPeriodUnit, LongConsumer warmUpConsumer,
			int testPeriod, TimeUnit testPeriodUnit, LongUnaryOperator testFunction
			) throws Exception{
		doRateTest(null, threadPool, numThreads, warmUpPeriod, warmUpPeriodUnit, warmUpConsumer,
				testPeriod, testPeriodUnit, testFunction);
	}
	public static void doRateTest(int numThreads, 
			int warmUpPeriod, TimeUnit warmUpPeriodUnit, LongConsumer warmUpConsumer,
			int testPeriod, TimeUnit testPeriodUnit, LongUnaryOperator testFunction
			) throws Exception{
		doRateTest(null, Executors.newFixedThreadPool(numThreads), numThreads, warmUpPeriod, warmUpPeriodUnit, warmUpConsumer,
				testPeriod, testPeriodUnit, testFunction);
	}
	
	public static void doRateTest(String title, int numThreads, 
			int warmUpPeriod, TimeUnit warmUpPeriodUnit, LongConsumer warmUpConsumer,
			int testPeriod, TimeUnit testPeriodUnit, LongUnaryOperator testFunction
			) throws Exception{
		doRateTest(title, Executors.newFixedThreadPool(numThreads), numThreads, warmUpPeriod, warmUpPeriodUnit, warmUpConsumer,
				testPeriod, testPeriodUnit, testFunction);
	}
		
	public static void doRateTest(String title, ExecutorService threadPool, int numThreads, 
			int warmUpPeriod, TimeUnit warmUpPeriodUnit, LongConsumer warmUpConsumer,
			int testPeriod, TimeUnit testPeriodUnit, LongUnaryOperator testFunction
			) throws Exception{
		LongConsumer warmUpFunction = warmUpConsumer != null? 
				warmUpConsumer : endTime -> testFunction.applyAsLong(endTime);
		
		long start = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(warmUpPeriod, warmUpPeriodUnit);
		long end = start + TimeUnit.MILLISECONDS.convert(testPeriod, testPeriodUnit);
		
		List<Future<Long>> futures = new LinkedList<>();
		for (int i = 0; i < numThreads; i ++){
			futures.add(threadPool.submit(()->{
				long total = 0;
				for (long now = System.currentTimeMillis(); now < end; now = System.currentTimeMillis()){
					if (now < start){ // warm up period
						warmUpFunction.accept(start);
					}else{
						total += testFunction.applyAsLong(end);
					}
				}
				return total;
			}));
		}
		
		final AtomicBoolean errorHappened = new AtomicBoolean(false);
		long total = futures.stream().mapToLong(f -> {
			try{
				return f.get();
			}catch(Exception e){
				e.printStackTrace();
				errorHappened.set(true);
				return 0;
			}
		}).sum();
		
		if (title != null){
			double rate = (double)total/TimeUnit.SECONDS.convert(testPeriod, testPeriodUnit);
			System.out.println("Rate of " + title + " is " + formatDouble("###,###.####", rate) + " per second" );
		}
		if (errorHappened.get()){
			System.out.println("Error happened during the test.");
		}
	}
	
	static public String formatDouble(String pattern, double value) {
		DecimalFormat myFormatter = new DecimalFormat(pattern);
		String output = myFormatter.format(value);
		return output;
	}

}
