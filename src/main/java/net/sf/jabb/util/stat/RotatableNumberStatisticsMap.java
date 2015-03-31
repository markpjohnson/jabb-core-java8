/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * 
 * @author James Hu
 *
 * @param <K>	type of the statistics key
 * @param <N>	type of the statistics numbers
 * @param <S>	type of the statistics
 */
public class RotatableNumberStatisticsMap<K, N extends Number, S extends NumberStatistics<N>> {
	
	class RotatedMap{
		long rotated; // milliseconds since epoch
		Map<K, S> map;
		
		RotatedMap(Map<K, S> map){
			this.map = map;
		}
	}
	
	protected RotatedMap current;
	protected Queue<RotatedMap> all;
	
	protected Supplier<Map<K, S>> mapSupplier;
	
	public RotatableNumberStatisticsMap(Supplier<Map<K, S>> mapSupplier){
		this.mapSupplier = mapSupplier;
		all = new ConcurrentLinkedQueue<>();
		
		current = new RotatedMap(createMap());
		all.add(current);
	}
	
	protected Map<K, S> createMap(){
		return mapSupplier.get();
	}
	
	/**
	 * Start using a new current map.
	 */
	synchronized public void rotate(){
		RotatedMap newMap = new RotatedMap(createMap());
		all.add(newMap);
		current.rotated = System.currentTimeMillis();
		current = newMap;
		
	}

	public Map<K, S> getCurrentMap() {
		return current.map;
	}
	
	/**
	 * Swap those rotated before a specific time.
	 * Since purged are no longer accessible for swapping, swapping should happen before purging. That means
	 * the millisecondsSinceEpoch argument of swapIfRotatedBefore should be smaller than that of purgeIfRotatedBefore.
	 * @param swapFunction		the function to do the swap, for example, the function can transform the original map 
	 * 							to a immutable one, or a disk-based persistent one. 
	 * @param millisecondsSinceEpoch	milliseconds since 1970-01-01 UTC
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be swapped.
	 */
	public void swapIfRotatedBefore(UnaryOperator<Map<K, S>> swapFunction, long millisecondsSinceEpoch){
		for (RotatedMap previous: all){		// The head of the queue is that element that has been on the queue the longest time.
			if (previous.rotated > 0 && previous.rotated < millisecondsSinceEpoch){
				Map<K, S> previousMap = previous.map;
				previous.map = swapFunction.apply(previousMap);
			}else{	// if reached the current one, or one rotated after the specified time
				break;
			}
		}
	}
	
	/**
	 * Return a list with those rotated before a specific time.
	 * @param millisecondsSinceEpoch	milliseconds since 1970-01-01 UTC
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be purged.
	 * @return	all those rotated before a specific time
	 */
	public List<Map<K, S>> listIfRotatedBefore(long millisecondsSinceEpoch){
		List<Map<K, S>> result = new LinkedList<>();
		for (RotatedMap previous: all){		// The head of the queue is that element that has been on the queue the longest time.
			if (previous.rotated > 0 && previous.rotated < millisecondsSinceEpoch){
				result.add(previous.map);
			}else{	// if reached the current one, or one rotated after the specified time
				break;
			}
		}
		return result;
	}

	
	/**
	 * Purge those rotated before a specific time.
	 * @param millisecondsSinceEpoch	milliseconds since 1970-01-01 UTC
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be purged.
	 */
	public void purgeIfRotatedBefore(long millisecondsSinceEpoch){
		while (true){
			RotatedMap previous = all.peek();
			if (previous.rotated > 0 && previous.rotated < millisecondsSinceEpoch){
				all.remove(previous);
			}else{	// if reached the current one, or one rotated after the specified time
				break;
			}
		}
	}

	/**
	 * Get the overall statistics which are the merged statistics of all the maps.
	 * @param key	the key
	 * @param filter  a filter to return false when a statistics object should be ignored. It can be null which means no filter needs to be applied
	 * @return	the merge statistics for the key
	 */
	public NumberStatistics<BigInteger> getOverallStatistics(K key, Predicate<S> filter) {
		NumberStatistics<BigInteger> result = new ConcurrentBigIntegerStatistics();
		for (RotatedMap previousOrCurrent: all){
			Map<K, S> map = previousOrCurrent.map;
			if (map.containsKey(key)){
				S statistics = map.get(key);
				if (filter == null || filter.test(statistics)){
					result.merge(statistics);
				}
			}
		}
		return result;
	}

	public void evaluate(K key, int value) {
		current.map.get(key).evaluate(value);
	}

	public void evaluate(K key, long value) {
		current.map.get(key).evaluate(value);
	}

	public void evaluate(K key, BigInteger value) {
		current.map.get(key).evaluate(value);
	}

	
	
}
