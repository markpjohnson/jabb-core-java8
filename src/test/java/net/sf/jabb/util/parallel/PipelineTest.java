/**
 * 
 */
package net.sf.jabb.util.parallel;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.junit.Test;

/**
 * @author James
 *
 */
public class PipelineTest {
	ExecutorService threadPool1 = Executors.newFixedThreadPool(5);
	ExecutorService threadPool2 = Executors.newFixedThreadPool(2);
	ExecutorService threadPool3 = Executors.newFixedThreadPool(4);
	ExecutorService threadPool4 = Executors.newFixedThreadPool(5);
	
	Function<String, Integer> stringToInteger = s -> Integer.valueOf(s);
	Function<Integer, Long> integerToLong = i -> {
		try{
			Thread.sleep(1000);
		}catch(InterruptedException e){
			Thread.interrupted();
		}
		return Long.valueOf(i);
	};
	Function<Long, BigInteger> longToBigInteger = l -> BigInteger.valueOf(l);
	Function<BigInteger, Integer> bigIntegerToInteger = big -> {
		if (big.compareTo(BigInteger.valueOf(-1)) == 0){
			throw new IllegalArgumentException("value -1 is not allowed");
		}
		return big.intValue();
	};

	@Test
	public void testSyntax() {
		PipelineRecursiveImpl<String, Integer> pipeline = createPipeline();
		assertNotNull(pipeline);
	};
	
	PipelineRecursiveImpl<String, Integer> createPipeline(){
		PipelineRecursiveImpl<String, Integer> pipeline = 
			//PipelineRecursiveImpl.outputTo(new LinkedList<Integer>())
			PipelineRecursiveImpl.<Integer>noOutput()
				.prepend(threadPool4, bigIntegerToInteger)
				.prepend(threadPool3, longToBigInteger)
				.prepend(threadPool2, integerToLong)
				.prepend(threadPool1, stringToInteger);
		return pipeline;
	}
	

	
	Integer createPipelineAndFeed(String input) throws InterruptedException, ExecutionException{
		PipelineRecursiveImpl<String, Integer> pipeline = createPipeline();
		Future<Integer> result = pipeline.feed(input);
		Integer output = result.get();
		return output;
	}
	
	@Test
	public void testResult() throws InterruptedException, ExecutionException{
		String input = "18763";
		Integer output = createPipelineAndFeed(input);
		assertEquals(input, output.toString());
	}

	@Test(expected=ExecutionException.class)
	public void testExecutionExceptionInStage1() throws InterruptedException, ExecutionException{
		String input = "18sdf763";
		createPipelineAndFeed(input);
	}

	@Test(expected=ExecutionException.class)
	public void testExecutionExceptionInStage4() throws InterruptedException, ExecutionException{
		String input = "-1";
		createPipelineAndFeed(input);
	}

	@Test(expected=CancellationException.class)
	public void testCancel() throws InterruptedException, ExecutionException{
		String input = "18763";
		PipelineRecursiveImpl<String, Integer> pipeline = createPipeline();
		Future<Integer> result = pipeline.feed(input);
		assertTrue(result.cancel(true));
		result.get();
	}
	
	@Test
	public void testFeedMore() throws InterruptedException, ExecutionException{
		PipelineRecursiveImpl<String, Integer> pipeline = createPipeline();
		
		int SIZE = 50;
		String[] inputs = new String[SIZE];
		List<Future<Integer>> outputs = new ArrayList<>(SIZE);
		for (int i = 0; i < SIZE; i ++){
			String input = String.valueOf(i);
			inputs[i] = input;
			outputs.add(pipeline.feed(input));
		}
		
		for (int i = 0; i < SIZE; i ++){
			assertEquals(inputs[i], outputs.get(i).get().toString());
		}
	}
}
