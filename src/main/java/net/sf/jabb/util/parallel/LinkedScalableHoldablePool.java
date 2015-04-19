/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * An ConcurrentLinkedQueue backed auto-scalable holdable pool. 
 * The pool will be empty until the first time getHold(...) is called.
 * When all of the objects in the pool are currently held, call to getHold(...) will result in a new object being created.
 * getHold(...) method of this class will never return null.
 * @author James Hu
 *
 */
public class LinkedScalableHoldablePool<T> implements HoldablePool<T> {
	protected Supplier<T> factory;
	protected Queue<Holdable<T>> pool;
	
	public LinkedScalableHoldablePool(Supplier<T> factory){
		this.factory = factory;
		this.pool = new ConcurrentLinkedQueue<>();
	}


	@Override
	public Holdable<T> getHold(long holdId) {
		for (Holdable<T> holdable: pool){
			if (holdable.hold(holdId)){
				return holdable;
			}
		}

		Holdable<T> holdable = new Holdable<>(factory.get(), holdId);
		pool.add(holdable);
		return holdable;
	}

	@Override
	public Collection<T> getAll() {
		return pool.stream().filter(h-> h != null).map(h -> h.get()).collect(Collectors.toList());
	}


	@Override
	public void reset(T object) {
		pool.clear();
		if (object != null){
			pool.add(new Holdable<>(object));
		}
		
	}


	@Override
	public int getSize() {
		return pool.size();
	}


	@Override
	public int getCapacity() {
		return Integer.MAX_VALUE;
	}

}
