/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Backed by LongAdder, this class can handle numbers larger than Long.MAX_VALUE.
 * It is multi-thread safe.
 * 
 * <p>This class extends {@link Number}, but does <em>not</em> define
 * methods such as {@code equals}, {@code hashCode} and {@code
 * compareTo} because instances are expected to be mutated, and so are
 * not useful as collection keys.
 * 
 * @author James Hu
 *
 */
public class BigIntegerAdder extends Number{
	private static final long serialVersionUID = -9214671662034042785L;
	protected static final int DEFAULT_NUM_VALUES = 5;
	protected static final BigInteger MINUS_ONE = BigInteger.ONE.negate();
	
	protected AtomicBigInteger[] values;
	
	//volatile 
	transient protected int j = 0;
	//transient protected int valueLength;
	
	public BigIntegerAdder(){
		this(DEFAULT_NUM_VALUES, BigInteger.ZERO);
	}
	
	public BigIntegerAdder(int concurrencyFactor){
		this(concurrencyFactor, BigInteger.ZERO);
	}

	public BigIntegerAdder(BigInteger initialValue){
		this(DEFAULT_NUM_VALUES, initialValue);
	}

	public BigIntegerAdder(int concurrencyFactor, BigInteger initialValue){
		if (concurrencyFactor < 1){
			throw new IllegalArgumentException("Concurrency factor must not be less than 1.");
		}
		//valueLength = concurrencyFactor;
		values = new AtomicBigInteger[concurrencyFactor];
		for (int i = 0; i < values.length; i ++){
			values[i] = new AtomicBigInteger(BigInteger.ZERO);
		}
		set(initialValue);
	}

    /**
     * Adds the given value.
     *
     * @param x the value to add
     */
    public void add(int x) {
    	if (x == 0){
    		return;
    	}
		values[j++ % values.length].add(BigInteger.valueOf(x));
    }
    
    /**
     * Adds the given value.
     *
     * @param x the value to add
     */
    public void add(long x) {
    	if (x == 0){
    		return;
    	}
		values[j++ % values.length].add(BigInteger.valueOf(x));
    }
    
	public void add(BigInteger x) {
		values[j++ % values.length].add(x);
	}
    
    /**
     * Equivalent to {@code add(1)}.
     */
    public void increment() {
		values[j++ % values.length].add(BigInteger.ONE);
    }

    /**
     * Equivalent to {@code add(-1)}.
     */
    public void decrement() {
		values[j++ % values.length].add(MINUS_ONE);
    }

    /**
     * Returns the current sum.  The returned value is <em>NOT</em> an
     * atomic snapshot; invocation in the absence of concurrent
     * updates returns an accurate result, but concurrent updates that
     * occur while the sum is being calculated might not be
     * incorporated.
     *
     * @return the sum
     */
    public BigInteger sum() {
    	BigInteger value = values[0].get();
    	for (int i = 1; i < values.length; i ++){
    		value = value.add(values[i].get());
    	}
    	return value;
    }
    
    /**
     * Resets the sum to zero. This method may
     * be a useful alternative to creating a new adder, but is only
     * effective if there are no concurrent updates.  Because this
     * method is intrinsically racy, it should only be used when it is
     * known that no threads are concurrently updating.
     */
    public void reset() {
    	set(BigInteger.ZERO);
    }
    
    /**
     * Set the sum to specified value. This method may
     * be a useful alternative to creating a new adder, but is only
     * effective if there are no concurrent updates.  Because this
     * method is intrinsically racy, it should only be used when it is
     * known that no threads are concurrently updating.
     * @param newValue the new value
     */
    public void set(BigInteger newValue) {
    	values[0].set(newValue);
		for (int i = 1; i < values.length; i ++){
			values[i].set(BigInteger.ZERO);
		}
     }
    
    /**
     * Equivalent in effect to {@link #sum} followed by {@link
     * #reset}. This method may apply for example during quiescent
     * points between multithreaded computations.  If there are
     * updates concurrent with this method, the returned value is
     * <em>not</em> guaranteed to be the final value occurring before
     * the reset.
     *
     * @return the sum
     */
    public BigInteger sumThenReset() {
    	BigInteger sum = sum();
    	reset();
    	return sum;
    }
    
    /**
     * Equivalent in effect to {@link #sum} followed by {@link
     * #set}. This method may apply for example during quiescent
     * points between multithreaded computations.  If there are
     * updates concurrent with this method, the returned value is
     * <em>not</em> guaranteed to be the final value occurring before
     * the set.
     *
     * @param newValue the new value
     * @return the sum
     */
    public BigInteger sumThenSet(BigInteger newValue) {
    	BigInteger sum = sum();
    	set(newValue);
    	return sum;
    }
    
    /**
     * Returns the String representation of the {@link #sum}.
     * @return the String representation of the {@link #sum}
     */
    public String toString() {
        return sum().toString();
    }


	@Override
	public int intValue() {
		return sum().intValue();
	}

	@Override
	public long longValue() {
		return sum().longValue();
	}

	@Override
	public float floatValue() {
		return sum().floatValue();
	}

	@Override
	public double doubleValue() {
		return sum().doubleValue();
	}
}
