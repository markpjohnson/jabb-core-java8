/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An array backed auto-scalable holdable pool. 
 * The pool will be empty until the first time getHold(...) is called.
 * When all of the objects in the pool are currently held, call to getHold(...) will result either a new object being created if the size limit has not been reached,
 * or an infinite loop until an object is released and can be held.
 * getHold(...) method of this class will never return null. 
 * @author James Hu
 *
 */
public class ArrayScalableHoldablePool<T> implements HoldablePool<T> {
	protected Supplier<T> factory;
	protected Holdable<T>[] pool;
	
	/**
	 * Constructor
	 * @param factory	the factory function to create the objects
	 * @param size		the maximum number of objects allowed to be craeted
	 */
	@SuppressWarnings("unchecked")
	public ArrayScalableHoldablePool(Supplier<T> factory, int size){
		this.factory = factory;
		this.pool = new Holdable[size];
	}
	
	/**
	 * Constructor. The size of the pool will be set to the same as the number of CPU cores.
	 * @param factory	the factory function to create the objects
	 */
	public ArrayScalableHoldablePool(Supplier<T> factory){
		this(factory, Runtime.getRuntime().availableProcessors());
	}

	@Override
	public Holdable<T> getHold(long holdId) {
		for (int i = 0; ; i = (i+1) % pool.length){
			Holdable<T> holdable = pool[i];
			if (holdable == null){
				synchronized(pool){
					if (pool[i] == null){
						holdable = new Holdable<>(factory.get(), holdId);
						pool[i] = holdable;
						return holdable;
					}
				}
			}else{
				if (holdable.hold(holdId)){
					return holdable;
				}
			}
		}
	}

	@Override
	public Collection<T> getAll() {
		return Arrays.stream(pool).filter(h-> h != null).map(h -> h.get()).collect(Collectors.toList());
	}

	@Override
	public void reset(T object) {
		for (int i = 0; i < pool.length; i ++){
			pool[i] = null;
		}
		
		if (object != null){
			pool[0] = new Holdable<>(object);
		}
		
	}

	@Override
	public int getSize() {
		for (int i = 0; i < pool.length; i ++){
			if (pool[i] == null){
				return i;
			}
		}
		return pool.length;
	}

	@Override
	public int getCapacity() {
		return pool.length;
	}


}
