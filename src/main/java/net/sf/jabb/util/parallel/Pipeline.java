/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author James Hu
 *
 */
public class Pipeline<I, O> {
	ExecutorService executor;
	Function<I, Future<O>> feedFunction;
	
	public static <O> Pipeline<O, O> endWith(Collection<O> outputCollection){
		return new Pipeline<O, O>(MoreExecutors.newDirectExecutorService(), input -> {
			outputCollection.add(input);
			return input;
		});
	}


	public Pipeline(ExecutorService executor, Function<I, O> function){
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
	
	public <OI> Pipeline(ExecutorService executor, Function<I, OI> function, Pipeline<OI, O> downstream){
		this.executor = executor;
		this.feedFunction = input -> {
			OI intermediate = function.apply(input);
			Future<O> outputFuture = downstream.feed(intermediate);
			return outputFuture;
		};
	}
	
	public <I0> Pipeline<I0, O> prepend(ExecutorService previousEexecutor, Function<I0, I> previousFunction){
		return new Pipeline<I0, O>(previousEexecutor, previousFunction, this);
	}
	
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
