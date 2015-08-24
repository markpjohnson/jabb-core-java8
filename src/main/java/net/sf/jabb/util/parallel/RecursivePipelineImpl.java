/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.util.concurrent.MoreExecutors;

/**
 * The simple and efficient recursive implementation of pipeline.
 * It requires you to build it in reversed order - from the last stage to the first stage.
 * 
 * @author James Hu
 *
 * @param <I>	type of input
 * @param <O>	type of output
 */
public class RecursivePipelineImpl<I, O> implements Pipeline<I, O>{
	ExecutorService executor;
	Function<I, Future<O>> feedFunction;
	
	/**
	 * Start building a pipeline, and specify a collection that all the final output will be put into.
	 * @param outputCollection	the collection that will be used to hold all the output.
	 * @param <O> type of output
	 * @return	a new pipeline that you can prepend stages
	 */
	public static <O> RecursivePipelineImpl<O, O> outputTo(Collection<O> outputCollection){
		return new RecursivePipelineImpl<O, O>(MoreExecutors.newDirectExecutorService(), input -> {
			outputCollection.add(input);
			return input;
		});
	}

	/**
	 * Start building a pipeline, and specify another pipeline that all the final output from this 
	 * pipleline will be fed to.
	 * @param downstreamPipeline	another pipeline
	 * @param <O> type of the output of this pipeline which is also the input of the downstream pipeline
	 * @param <O2> type of the output of the downstream pipeline
	 * @return	a new pipeline that you can prepend stages
	 */
	public static <O, O2> RecursivePipelineImpl<O, Pipeline.IntermediateOutput<O, O2>> outputTo(Pipeline<O, O2> downstreamPipeline){
		return new RecursivePipelineImpl<O, Pipeline.IntermediateOutput<O, O2>>(MoreExecutors.newDirectExecutorService(), input -> {
			Future<O2> future = downstreamPipeline.feed(input);
			Pipeline.IntermediateOutput<O, O2> intermediateOutput = new Pipeline.IntermediateOutput<O, O2>(input, future);
			return intermediateOutput;
		});
	}

	/**
	 * Start building a pipleline that doesn't put the final output anywhere.
	 * Usage example: <code>PipelineRecursiveImpl.&lt;Integer&gt;noOutput()</code>
	 * @param <O> type of the output
	 * @return a pipeline that does not output to anywhere
	 */
	public static <O> RecursivePipelineImpl<O, O> noOutput(){
		return new RecursivePipelineImpl<O, O>(MoreExecutors.newDirectExecutorService(), input -> input);
	}


	private RecursivePipelineImpl(ExecutorService executor, Function<I, O> function){
		this.executor = executor;
		this.feedFunction = input -> {
			O output = function.apply(input);
			Future<O> outputFuture = new Future<O>(){		// this future is actually present!

				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					return false;
				}

				@Override
				public boolean isCancelled() {
					return false;
				}

				@Override
				public boolean isDone() {
					return true;
				}

				@Override
				public O get() throws InterruptedException, ExecutionException {
					return output;
				}

				@Override
				public O get(long timeout, TimeUnit unit)
						throws InterruptedException, ExecutionException,
						TimeoutException {
					return output;
				}
				
			};
			return outputFuture;
		};
	}
	
	private <OI> RecursivePipelineImpl(ExecutorService executor, Function<I, OI> function, RecursivePipelineImpl<OI, O> downstream){
		this.executor = executor;
		this.feedFunction = input -> {
			OI intermediate = function.apply(input);
			Future<O> outputFuture = downstream.feed(intermediate);
			return outputFuture;
		};
	}
	
	/**
	 * Prepend a stage to the pipeline
	 * @param executor		the executor service for the processing in the prepended stage
	 * @param function		the function to be applied in the prepended stage
	 * @param <I0> type of the new input
	 * @return		the new pipeline with the stage prepended
	 */
	public <I0> RecursivePipelineImpl<I0, O> prepend(ExecutorService executor, Function<I0, I> function){
		return new RecursivePipelineImpl<I0, O>(executor, function, this);
	}
	
	/**
	 * Prepend a stage to the pipeline. A new thread pool will be created and that thread pool will only be eligible for 
	 * garbage collection when the pipleline itself is eligible for garbage collection. 
	 * @param fixedThreadPoolSize		the thread pool size of a fixed size thread pool which will be used for the processing in the prepended stage
	 * @param function		the function to be applied in the prepended stage
	 * @param <I0> type of the new input
	 * @return		the new pipeline with the stage prepended
	 */
	public <I0> RecursivePipelineImpl<I0, O> prepend(int fixedThreadPoolSize, Function<I0, I> function){
		ExecutorService executor = Executors.newFixedThreadPool(fixedThreadPoolSize);
		return new RecursivePipelineImpl<I0, O>(executor, function, this);
	}
	
	@Override
	public Future<O> feed(I input){
		 Future<Future<O>> futureOfFuture = executor.submit(()->feedFunction.apply(input));
		 return new Future<O>(){

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				if (futureOfFuture.cancel(mayInterruptIfRunning)){
					return true;
				}
				if (!futureOfFuture.isCancelled()){	// is done normally
					try {
						Future<O> future = futureOfFuture.get();
						return future.cancel(mayInterruptIfRunning);
					} catch (InterruptedException | ExecutionException e) {
						// just ignore exceptions;
						return true;
					}
				}else{ // already been cancelled
					return false;
				}
			}

			@Override
			public boolean isCancelled() {
				if (futureOfFuture.isCancelled()){
					return true;
				}else{
					if (futureOfFuture.isDone()){
						Future<O> future;
						try {
							future = futureOfFuture.get();
							return future.isCancelled();
						} catch (CancellationException e){
							return true;
						} catch (InterruptedException | ExecutionException e) {
							return false;
						}
					}else{
						return false;
					}
				}
			}

			@Override
			public boolean isDone() {
				if (futureOfFuture.isDone()){
					try {
						Future<O> future = futureOfFuture.get();
						return future.isDone();
					} catch (InterruptedException | ExecutionException e) {
						// just ignore exceptions;
						return true;
					}
				}else{
					return false;
				}
			}

			@Override
			public O get() throws InterruptedException, ExecutionException {
				Future<O> future = futureOfFuture.get();
				return future.get();
			}

			@Override
			public O get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				long remainingNanos = unit.toNanos(timeout);
			    long end = System.nanoTime() + remainingNanos;
			    
			    Future<O> future = futureOfFuture.get(timeout, unit);
			    remainingNanos = end - System.nanoTime();
			    
			    if (remainingNanos > 0){
			    	return future.get(remainingNanos, TimeUnit.NANOSECONDS);
			    }else{
			    	throw new TimeoutException();
			    }
			}
			 
		 };
	}
}
