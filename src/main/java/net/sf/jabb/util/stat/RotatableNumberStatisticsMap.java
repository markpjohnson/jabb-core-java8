/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
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
	 * Get number of rotated, exclude current.
	 * @return	number of rotated, always greater than or equals to 0
	 */
	public int getRotatedSize(){
		return all.size() - 1;
	}
	
	/**
	 * Swap those rotated between a specific time period. The current map will not be swapped.
	 * Since purged are no longer accessible for swapping, for any specific element, swapping should happen before purging. 
	 * @param swapFunction		the function to do the swap, for example, the function can transform the original map 
	 * 							to a immutable one, or a disk-based semi-persistent one. The swap function should not change the statistics.
	 * @param rotatedBegin	milliseconds since 1970-01-01 UTC, inclusive.
	 * 								rotatedBegin should be less than roatedEnd and should not be zero
	 * @param rotatedEnd	milliseconds since 1970-01-01 UTC, exclusive.
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be swapped.
	 */
	public void swap(UnaryOperator<Map<K, S>> swapFunction, long rotatedBegin, long rotatedEnd){
		for (RotatedMap previousOrCurrent: all){		// The head of the queue is the element that has been on the queue the longest time.
			if (previousOrCurrent.rotated < rotatedBegin){
				continue;
			}else if (previousOrCurrent.rotated >= rotatedEnd){
				break;
			}else{
				Map<K, S> previousMap = previousOrCurrent.map;
				previousOrCurrent.map = swapFunction.apply(previousMap);
			}
		}
	}
	
	/**
	 * Return a list with those rotated between a specific time period. The current map will not be included in the list.
	 * @param rotatedBegin	milliseconds since 1970-01-01 UTC, inclusive.
	 * 								rotatedBegin should be less than roatedEnd and should not be zero
	 * @param rotatedEnd	milliseconds since 1970-01-01 UTC, exclusive.
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be swapped.
	 * @return	all those rotated between the time period. The elements are in the order they were rotated, from earliest to latest.
	 */
	public List<Map<K, S>> getRotatedMaps(long rotatedBegin, long rotatedEnd){
		List<Map<K, S>> result = new LinkedList<>();
		for (RotatedMap previousOrCurrent: all){		// The head of the queue is the element that has been on the queue the longest time.
			if (previousOrCurrent.rotated < rotatedBegin){
				continue;
			}else if (previousOrCurrent.rotated >= rotatedEnd){
				break;
			}else{
				result.add(previousOrCurrent.map);
			}
		}
		return result;
	}

	
	/**
	 * Purge those rotated before a specific time.
	 * @param finalizeFunction		the function to be applied to the map after it has been purged. It can be null if not necessary.
	 * @param millisecondsSinceEpoch	milliseconds since 1970-01-01 UTC
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the maps to be purged.
	 */
	public void purge(Consumer<Map<K, S>> finalizeFunction, long millisecondsSinceEpoch){
		while (true){
			RotatedMap previous = all.peek();
			if (previous.rotated > 0 && previous.rotated < millisecondsSinceEpoch){
				all.remove(previous);
				if (finalizeFunction != null){
					finalizeFunction.accept(previous.map);
				}
			}else{	// if reached the current one, or one rotated after the specified time
				break;
			}
		}
	}

	/**
	 * Get the overall statistics which are the merged statistics of all the maps.
	 * @param key	the key
	 * @param filter  a filter to return false when a statistics object should be ignored. It can be null which means no filter needs to be applied
	 * @return	the merged statistics for the key
	 */
	public NumberStatistics<BigInteger> getStatistics(K key, Predicate<S> filter) {
		NumberStatistics<BigInteger> result = new ConcurrentBigIntegerStatistics(1);
		for (RotatedMap previousOrCurrent: all){
			Map<K, S> map = previousOrCurrent.map;
			S statistics = map.get(key);
			if (statistics != null && (filter == null || filter.test(statistics))){
				result.merge(statistics);
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
