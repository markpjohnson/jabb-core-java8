/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
	
/**
 * Dispatcher to be used in non-clustered load balancing. This class is thread safe.
 * @author James Hu
 *
 * @param <L>	type of the load
 * @param <R>	type of the result
 * @param <P>	type of the processor
 */
public class BasicLoadBalancer<L, R, P> implements LoadBalancer<L, R, P> {
	protected BiFunction<P, L, R> dispatcher;
	protected ToIntFunction<L> hashFunction;
	protected Consumer<L> fallback;
	protected Consumer<DispatchingStatistics<L, P>> statistics;
	protected Consumer<L> monitor;
	
	protected Object[] processorMap;
	protected List<P> activeProcessors;
	protected List<P> backupProcessors;
	
	protected Random random = new Random(System.currentTimeMillis());

	
	/**
	 * Constructor
	 * @param buckets	number of buckets for distributing the load evenly, normally it should be at least 10 times bigger than the number of processors. Numbers larger than 100000 suggested.
	 * @param activeProcessors  processors, there must not be duplicated elements
	 * @param hashFunction	the function to create hash code for the work load patching
	 * @param dispatcher	actual dispatcher
	 */
	public BasicLoadBalancer(int buckets, Collection<P> activeProcessors, ToIntFunction<L> hashFunction, BiFunction<P, L, R> dispatcher){
		this(buckets, activeProcessors, null, null, hashFunction, dispatcher, null, null);
	}

	
	/**
	 * Constructor
	 * @param buckets	number of buckets for distributing the load evenly, normally it should be at least 10 times bigger than the number of processors. Numbers larger than 100000 suggested.
	 * @param activeProcessors  processors, there must not be duplicated elements
	 * @param backupProcessors	backup processors which will not be used at the beginning, there must not be duplicated elements. It can be null.
	 * @param fallback	the fall back processor that handles all the failed-to-dispatch work loads. It can be null.
	 * @param hashFunction	the function to create hash code for the work load dispatching
	 * @param dispatcher	actual dispatcher
	 * @param statistics	statistics collector. It can be null.
	 * @param monitor		monitor that receives copies of all of the work load. It can be null.
	 */
	public BasicLoadBalancer(int buckets, Collection<P> activeProcessors, Collection<P> backupProcessors, Consumer<L> fallback, ToIntFunction<L> hashFunction, BiFunction<P, L, R> dispatcher, Consumer<DispatchingStatistics<L, P>> statistics, Consumer<L> monitor){
		initialize(buckets, activeProcessors, backupProcessors, fallback, hashFunction, dispatcher, statistics, monitor);
	}
	
	protected void initialize(int buckets, Collection<P> activeProcessors, Collection<P> backupProcessors, Consumer<L> fallback, ToIntFunction<L> hashFunction, BiFunction<P, L, R> dispatcher, Consumer<DispatchingStatistics<L, P>> statistics, Consumer<L> monitor){
		if (activeProcessors == null || activeProcessors.size() < 1){
			throw new IllegalArgumentException("At least one processor is required");
		}
		
		if (buckets < activeProcessors.size() * 10){
			throw new IllegalArgumentException("Number of buckets should be greater than at least ten times the number of active processors");
		}

		this.activeProcessors = new LinkedList<>(activeProcessors);
		this.backupProcessors = new LinkedList<>(backupProcessors == null? Collections.emptyList() : backupProcessors);
		this.fallback = fallback;
		this.processorMap = new Object[buckets];
		this.hashFunction = hashFunction;
		this.dispatcher = dispatcher;
		this.statistics = statistics;
		this.monitor = monitor;
		
		int numActiveProcessors = activeProcessors.size();
		for (int i = 0; i < processorMap.length; i ++){
			processorMap[i] = this.activeProcessors.get(i % numActiveProcessors);
		}
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#increaseLoad(P, float)
	 */
	@Override
	synchronized public void increaseLoad(P processor, float percentageDelta){
		if (activeProcessors.size() < 2){
			return;
		}
		int buckets = (int)(percentageDelta * processorMap.length);
		if (buckets < 1){
			return;
		}
		
		for (int i = 0; i < buckets; i ++){	
			int j = random.nextInt(processorMap.length);
			for (int k = 0; processorMap[j] == processor; k ++){
				j = (j+1) % processorMap.length;
				if (k > processorMap.length){
					return;		// no more
				}
			}
			processorMap[j] = processor;
		}
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#decreaseLoad(P, float)
	 */
	@Override
	synchronized public void decreaseLoad(P processor, float percentageDelta){
		if (activeProcessors.size() < 2){
			return;
		}
		int buckets = (int)(percentageDelta * processorMap.length);
		if (buckets < 1){
			return;
		}
		
		List<P> otherProcessors = new ArrayList<>(activeProcessors);
		otherProcessors.remove(processor);
		for (int i = 0; i < buckets; i ++){	
			int j = random.nextInt(processorMap.length);
			for (int k = 0; processorMap[j] != processor; k ++){
				j = (j+1) % processorMap.length;
				if (k > processorMap.length){
					return;		// no more
				}
			}
			P anotherProcessor = otherProcessors.get(i % otherProcessors.size());
			processorMap[j] = anotherProcessor;
		}
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#add(P)
	 */
	@Override
	synchronized public void add(P processor){
		activeProcessors.add(processor);
		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < processorMap.length/activeProcessors.size(); i ++){
			int j = random.nextInt(processorMap.length);
			for (int k = 0; processorMap[j] == processor; k ++){
				j = (j+1) % processorMap.length;
				if (k > processorMap.length){
					return;		// no more
				}
			}
			processorMap[j] = processor;
		}
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#remove(P)
	 */
	@Override
	synchronized public void remove(P processor){
		activeProcessors.remove(processor);
		int numActiveProcessors = activeProcessors.size();
		for (int i = 0, j = 0; i < processorMap.length; i ++){
			if (processor.equals(processorMap[i])){
				processorMap[i] = activeProcessors.get(j++ % numActiveProcessors);
			}
		}
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#addBackup(P)
	 */
	@Override
	synchronized public void addBackup(P processor){
		backupProcessors.add(processor);
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#removeBackup(P)
	 */
	@Override
	synchronized public void removeBackup(P processor){
		backupProcessors.remove(processor);
	}

	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#promote(P)
	 */
	@Override
	synchronized public void promote(P processor){
		removeBackup(processor);
		add(processor);
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#demote(P)
	 */
	@Override
	synchronized public void demote(P processor){
		remove(processor);
		addBackup(processor);
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#replace(P, P)
	 */
	@Override
	synchronized public void replace(P processor, P newProcessor){
		if (!activeProcessors.remove(processor)){
			throw new IllegalArgumentException("The processor to be replaced cannot be found: " + processor);
		}
		for (int i = 0; i < processorMap.length; i ++){
			if (processor.equals(processorMap[i])){
				processorMap[i] = newProcessor;
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.LoadBalancer#replaceBackup(P, P)
	 */
	@Override
	synchronized public void replaceBackup(P processor, P newProcessor){
		if (!backupProcessors.remove(processor)){
			throw new IllegalArgumentException("The processor to be replaced cannot be found: " + processor);
		}
		backupProcessors.add(newProcessor);
	}
	
	@SuppressWarnings("unchecked")
	protected P chooseProcessor(L load){
		int hash = 0x7fffffff & hashFunction.applyAsInt(load);
		P[] processors = (P[])processorMap;
		return processors[hash % processors.length];
	}
	
	/* (non-Javadoc)
	 * @see net.sf.jabb.util.parallel.Dispatcher#dispatch(L)
	 */
	@Override
	public R dispatch(L load){
		long startTime = System.currentTimeMillis();
		boolean successful = false;
		Integer exceptionType = null;
		P processor = null;
		try{
			processor = chooseProcessor(load);
			R result = dispatcher.apply(processor, load);
			successful = true;
			return result;
		}catch(DispatchingException e){
			exceptionType = e.getType();
			if (fallback != null){
				try{
					fallback.accept(load);
				}catch(Exception fe){
					// ignore
					fe.printStackTrace();
				}
			}
			throw e;
		}finally{
			if (statistics != null){
				try{
					statistics.accept(new DispatchingStatistics<L, P>(load, processor, System.currentTimeMillis() - startTime, successful, exceptionType));
				}catch(Exception se){
					// ignore
					se.printStackTrace();
				}
			}
			if (monitor != null){
				try{
					monitor.accept(load);
				}catch(Exception me){
					// ignore
					me.printStackTrace();
				}
			}
		}
	}
}
