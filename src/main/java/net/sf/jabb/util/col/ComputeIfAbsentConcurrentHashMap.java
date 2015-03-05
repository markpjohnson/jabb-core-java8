/**
 * 
 */
package net.sf.jabb.util.col;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A ConcurrentHashMap that always automatically do computeIfAbsent(...) within get(...)
 * @author James Hu
 *
 */
public class ComputeIfAbsentConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V>{
	private static final long serialVersionUID = 6421468008670367495L;
	
	protected Function<? super K,? extends V> computeFunction;

    /**
     * Creates a new, empty map with the default initial table size (16).
     * 
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentHashMap(Function<? super K,? extends V> computeFunction) {
    	super();
    	this.computeFunction = computeFunction;
    }

    /**
     * Creates a new, empty map with an initial table size
     * accommodating the specified number of elements without the need
     * to dynamically resize.
     *
     * @param initialCapacity The implementation performs internal
     * sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentHashMap(int initialCapacity, Function<? super K,? extends V> computeFunction) {
    	super(initialCapacity);
    	this.computeFunction = computeFunction;
    }

    /**
     * Creates a new map with the same mappings as the given map.
     *
     * @param m the map
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentHashMap(Map<? extends K, ? extends V> m, Function<? super K,? extends V> computeFunction) {
    	super(m);
    	this.computeFunction = computeFunction;
    }

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}) and
     * initial table density ({@code loadFactor}).
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements,
     * given the specified load factor.
     * @param loadFactor the load factor (table density) for
     * establishing the initial table size
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative or the load factor is nonpositive
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentHashMap(int initialCapacity, float loadFactor, Function<? super K,? extends V> computeFunction) {
    	super(initialCapacity, loadFactor);
    	this.computeFunction = computeFunction;
    }

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}), table
     * density ({@code loadFactor}), and number of concurrently
     * updating threads ({@code concurrencyLevel}).
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements,
     * given the specified load factor.
     * @param loadFactor the load factor (table density) for
     * establishing the initial table size
     * @param concurrencyLevel the estimated number of concurrently
     * updating threads. The implementation may use this value as
     * a sizing hint.
     * @throws IllegalArgumentException if the initial capacity is
     * negative or the load factor or concurrencyLevel are
     * nonpositive
     * @param computeFunction the function to compute a value when there is no value currently associated with the key
     */
    public ComputeIfAbsentConcurrentHashMap(int initialCapacity,
                             float loadFactor, int concurrencyLevel, Function<? super K,? extends V> computeFunction) {
    	super(initialCapacity, loadFactor, concurrencyLevel);
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
