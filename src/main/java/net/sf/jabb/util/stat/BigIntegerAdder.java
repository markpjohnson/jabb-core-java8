/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;
import java.util.concurrent.ConcurrentSkipListSet;
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
	private static long THRESHOLD = Long.MAX_VALUE / 2;
	
	/**
	 * The class to wrap LongAdder with Comparable interface.
	 * @author James Hu
	 *
	 */
	private static class AdderWrapper implements Comparable<AdderWrapper>{
		private LongAdder adder;
		private long createdTime;
		
		private AdderWrapper(){
			this.adder = new LongAdder();
			this.createdTime = System.currentTimeMillis();
		}

		@Override
		public int compareTo(AdderWrapper o) {
			return (int)(this.createdTime - o.createdTime);
		}
	}

	private AtomicBigInteger baseValue;
	private ConcurrentSkipListSet<AdderWrapper> adders = new ConcurrentSkipListSet<>();
	private LongAdder adder;
	
	public BigIntegerAdder(){
		this(BigInteger.ZERO);
	}
	
	public BigIntegerAdder(BigInteger initialValue){
		baseValue = new AtomicBigInteger(initialValue);
		addNewAdder();
	}
	
	private LongAdder addNewAdder(){
		AdderWrapper newAdderWrapper = new AdderWrapper();
		LongAdder newAdder = newAdderWrapper.adder;
		adder = newAdder;
		adders.add(newAdderWrapper);
		return newAdder;
	}
	
    /**
     * Adds the given value.
     *
     * @param x the value to add
     */
    public void add(long x) {
    	if (x >= THRESHOLD){
    		add(BigInteger.valueOf(x));
    	}else{
         	if (adder.longValue() >= THRESHOLD){
        		addNewAdder().add(x);
        	}else{
            	adder.add(x);
        	}
    	}
    }
    
	public void add(BigInteger x) {
		baseValue.addAndGet(x);
	}
    
    /**
     * Equivalent to {@code add(1)}.
     */
    public void increment() {
        add(1L);
    }

    /**
     * Equivalent to {@code add(-1)}.
     */
    public void decrement() {
        add(-1L);
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
    	BigInteger value = baseValue.get();
    	for (AdderWrapper wrapper: adders){
    		value = value.add(BigInteger.valueOf(wrapper.adder.longValue()));
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
    	baseValue.set(newValue);
    	adders.clear();
    	addNewAdder();
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
