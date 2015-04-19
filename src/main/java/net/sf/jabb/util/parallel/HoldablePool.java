/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.Collection;


/**
 * A pool of holdable objects
 * @author James Hu
 *
 */
public interface HoldablePool<T> {

	/**
	 * Get and held object from the pool
	 * @param holdId the unique non-negative value to identify the caller
	 * @return the object that is held by the caller, or null the request cound't be fulfilled
	 */
	public Holdable<T> getHold(long holdId);
	
	/**
	 * Get all the object. This method should only be called when the pool is not in use.
	 * @return	a collection containing all the objects.
	 */
	public Collection<T> getAll();
	
	/**
	 * Get current size
	 * @return	number of instances in the pool
	 */
	public int getSize();
	
	/**
	 * Get the capacity of the pool
	 * @return	the maximum number of instances allowed/supported
	 */
	public int getCapacity();
	
	/**
	 * Reset the pool by removing all existing objects and then put one initial object into the pool.
	 * @param object	the initial object. If it is null then the pool will be empty
	 */
	public void reset(T object);
	
	/**
	 * Reset the pool by removing all existing objects
	 */
	default public void reset(){
		reset(null);
	}
	
	/**
	 * Release the held object to the pool.
	 * @param holdable	the object that will be set free
	 */
	default public void release(Holdable<T> holdable){
		holdable.free();
	}
	
	/**
	 * Get and hold object from the pool. The ID of current thread is used to identify the caller
	 * @return	the object that is held by the caller, or null the request cound't be fulfilled
	 */
	default public Holdable<T> getHold(){
		return getHold(Thread.currentThread().getId());
	}

}
