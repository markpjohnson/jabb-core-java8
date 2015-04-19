/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper for an object that can be held/locked by a thread
 * @author James Hu
 *
 */
public class Holdable<T> {
	public static final long FREE = -1;
	protected AtomicLong holdId;
	protected T object;
	
	/**
	 * Constructor. The instance created will initially be held by the holdId specified.
	 * @param object	the object
	 * @param holdId	the initial holdId which cannot be negative.
	 */
	Holdable(T object, long holdId){
		this.object = object;
		this.holdId = new AtomicLong(holdId);
	}
	
	/**
	 * Constructor. The instance created will initially be free.
	 * @param object	the object
	 */
	Holdable(T object){
		this(object, FREE);
	}
	
	/**
	 * Get the object
	 * @return the object
	 */
	public T get(){
		return object;
	}
	
	/**
	 * Try to hold the object
	 * @param holdId	an unique non-negative value
	 * @return	true if successfully held the object, false if the object is being held by someone else.
	 */
	boolean hold(long holdId){
		return this.holdId.compareAndSet(FREE, holdId);
	}
	
	/**
	 * Free the object.
	 */
	void free(){
		this.holdId.set(FREE);
	}
}
