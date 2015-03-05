/**
 * 
 */
package net.sf.jabb.util.col;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

/**
 * A ConcurrentSkipListMap that always automatically do computeIfAbsent(...) within get(...)
 * @author James Hu
 *
 */
public class ComputeIfAbsentConcurrentSkipListMap<K, V> extends ConcurrentSkipListMap<K, V>{
	private static final long serialVersionUID = 3897881578863979465L;

	protected Function<? super K,? extends V> computeFunction;

    /**
     * Constructs a new, empty map, sorted according to the
     * {@linkplain Comparable natural ordering} of the keys.
     * 
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentSkipListMap(Function<? super K,? extends V> computeFunction) {
    	super();
    	this.computeFunction = computeFunction;
    }

    /**
     * Constructs a new, empty map, sorted according to the specified
     * comparator.
     *
     * @param comparator the comparator that will be used to order this map.
     *        If {@code null}, the {@linkplain Comparable natural
     *        ordering} of the keys will be used.
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentSkipListMap(Comparator<? super K> comparator, Function<? super K,? extends V> computeFunction) {
    	super(comparator);
    	this.computeFunction = computeFunction;
    }

    /**
     * Constructs a new map containing the same mappings as the given map,
     * sorted according to the {@linkplain Comparable natural ordering} of
     * the keys.
     *
     * @param  m the map whose mappings are to be placed in this map
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     * @throws NullPointerException if the specified map or any of its keys
     *         or values are null
     */
    public ComputeIfAbsentConcurrentSkipListMap(Map<? extends K, ? extends V> m, Function<? super K,? extends V> computeFunction) {
    	super(m);
    	this.computeFunction = computeFunction;
    }

    /**
     * Constructs a new map containing the same mappings and using the
     * same ordering as the specified sorted map.
     *
     * @param m the sorted map whose mappings are to be placed in this
     *        map, and whose comparator is to be used to sort this map
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     * @throws NullPointerException if the specified map or any of its keys
     *         or values are null
     */
    public ComputeIfAbsentConcurrentSkipListMap(SortedMap<K, ? extends V> m, Function<? super K,? extends V> computeFunction) {
    	super(m);
    	this.computeFunction = computeFunction;
    }

    
    /**
     * Returns the value to which the specified key is mapped.
     * A new value will be computed and put into the map if there is no value for that key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key.equals(k)},
     * then this method returns {@code v}; otherwise it computes a new value and returns it.
     *  (There can be at most one such mapping.)
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    @SuppressWarnings("unchecked")
	@Override
    public V get(Object key){
    	return computeIfAbsent((K)key, computeFunction);
    }

}
