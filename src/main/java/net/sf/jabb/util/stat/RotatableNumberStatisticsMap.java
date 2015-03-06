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
import java.util.function.Supplier;

/**
 * @author James Hu
 *
 */
public class RotatableNumberStatisticsMap<K, N extends Number> {
	
	static class RotatedMap<K, N extends Number>{
		long rotated; // milliseconds since epoch
		Map<K, NumberStatistics<N>> map;
		
		RotatedMap(Map<K, NumberStatistics<N>> map){
			this.map = map;
		}
	}
	
	protected RotatedMap<K, N> current;
	protected Queue<RotatedMap<K, N>> all;
	
	protected Supplier<Map<K, NumberStatistics<N>>> mapSupplier;
	
	public RotatableNumberStatisticsMap(Supplier<Map<K, NumberStatistics<N>>> mapSupplier){
		this.mapSupplier = mapSupplier;
		all = new ConcurrentLinkedQueue<>();
		
		current = new RotatedMap<K, N>(createMap());
		all.add(current);
	}
	
	protected Map<K, NumberStatistics<N>> createMap(){
		return mapSupplier.get();
	}
	
	/**
	 * Start using a new current map.
	 */
	synchronized public void rotate(){
		RotatedMap<K, N> newMap = new RotatedMap<K, N>(createMap());
		all.add(newMap);
		current.rotated = System.currentTimeMillis();
		current = newMap;
		
	}

	public Map<K, NumberStatistics<N>> getCurrentMap() {
		return current.map;
	}
	
	/**
	 * Purge those rotated before a specific time.
	 * @param millisecondsSinceEpoch	milliseconds since 1970-01-01 UTC
	 * @return	all those purged merged into a single map, can be empty, never null.
	 */
	public Map<K, NumberStatistics<N>> purgeIfRotatedBefore(long millisecondsSinceEpoch){
		List<Map<K, NumberStatistics<N>>> purged = new LinkedList<>();
		while (true){
			RotatedMap<K, N> previous = all.peek();
			if (previous.rotated > 0 && previous.rotated < millisecondsSinceEpoch){
				all.remove(previous);
				purged.add(previous.map);
			}else{	// if reached the current one, or one rotated before the specified time
				break;
			}
		}
		
		if (purged.size() == 0){
			return Collections.emptyMap();
		}
		
		Map<K, NumberStatistics<N>> result = purged.get(0);
		ListIterator<Map<K, NumberStatistics<N>>> iterator = purged.listIterator(1);
		while(iterator.hasNext()){
			Map<K, NumberStatistics<N>> map = iterator.next();
			for (Map.Entry<K, NumberStatistics<N>> entry: map.entrySet()){
				result.get(entry.getKey()).merge(entry.getValue());
			}
		}
		return result;
	}

	/**
	 * Get the overall statistics which are the merged statistics of all the maps.
	 * @param key	the key
	 * @return	the merge statistics for the key
	 */
	public NumberStatistics<BigInteger> getOverallStatistics(K key) {
		NumberStatistics<BigInteger> result = new ConcurrentBigIntegerStatistics();
		for (RotatedMap<K, N> previousOrCurrent: all){
			Map<K, NumberStatistics<N>> map = previousOrCurrent.map;
			if (map.containsKey(key)){
				NumberStatistics<N> statistics = map.get(key);
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
