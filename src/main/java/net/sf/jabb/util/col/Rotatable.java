/**
 * 
 */
package net.sf.jabb.util.col;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;


/**
 * A series of objects containing a current one and multiple rotated ones.
 * @author James Hu
 *
 */
public class Rotatable<T> {
	public static class Wrapper<E>{
		long rotated; // rotated time - milliseconds since epoch UTC, or 0 if never rotated
		public E obj;
		
		Wrapper(E obj){
			this.obj = obj;
		}
	}

	protected Wrapper<T> current;
	protected Queue<Wrapper<T>> all;
	
	protected Supplier<T> objFactory;

	/**
	 * Constructor
	 * @param objFactory	factory function of the object
	 */
	public Rotatable(Supplier<T> objFactory){
		this.objFactory = objFactory;
		all = new ConcurrentLinkedQueue<>();
		
		current = new Wrapper<>(objFactory.get());
		all.add(current);
	}
	
	
	/**
	 * Start using a new current map.
	 */
	synchronized public void rotate(){
		Wrapper<T> newOne = new Wrapper<>(objFactory.get());
		all.add(newOne);
		current.rotated = System.currentTimeMillis();  // previous current
		current = newOne;	// new current
		
	}

	/**
	 * Get the current one
	 * @return	the current one
	 */
	public T getCurrent() {
		return current.obj;
	}
	
	/**
	 * Get the rotated and the current
	 * @return	all
	 */
	public List<T> getAll(){
		return all.stream().map(wrapper->wrapper.obj).collect(Collectors.toList());
	}
	
	/**
	 * Get number of rotated, exclude current.
	 * @return	number of rotated, always greater than or equals to 0
	 */
	public int getRotatedSize(){
		return all.size() - 1;
	}
	
	/**
	 * Swap those rotated between a specific time period. The current one will never be swapped.
	 * Since purged are no longer accessible for swapping, swapping should happen before purging for any object. 
	 * @param swapFunction		the function to do the swap, for example, the function can transform the original map 
	 * 							to a immutable one, or a disk-based semi-persistent one. The swap function should not change the value the original one represents.
	 * @param rotatedNoEarlierThan	milliseconds since 1970-01-01 UTC, inclusive.
	 * 								rotatedBegin should be less than roatedEnd and should not be zero
	 * @param rotatedBefore	milliseconds since 1970-01-01 UTC, exclusive.
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the object to be swapped.
	 */
	public void swap(UnaryOperator<T> swapFunction, long rotatedNoEarlierThan, long rotatedBefore){
		for (Wrapper<T> previousOrCurrent: all){		// The head of the queue is the element that has been on the queue the longest time.
			if (previousOrCurrent.rotated < rotatedNoEarlierThan){
				continue;
			}else if (previousOrCurrent.rotated >= rotatedBefore){
				break;
			}else{
				T obj = previousOrCurrent.obj;
				previousOrCurrent.obj = swapFunction.apply(obj);
			}
		}
	}
	
	/**
	 * Return a list with those rotated between a specific time period. The current one will never be included in the list.
	 * @param rotatedNoEarlierThan	milliseconds since 1970-01-01 UTC, inclusive.
	 * 								rotatedBegin should be less than roatedEnd and should not be zero
	 * @param rotatedBefore	milliseconds since 1970-01-01 UTC, exclusive.
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the object to be swapped.
	 * @return	all those rotated between the time period. The objects are in the order they were rotated, from earliest to latest.
	 */
	public List<T> getRotated(long rotatedNoEarlierThan, long rotatedBefore){
		List<T> result = new LinkedList<>();
		for (Wrapper<T> previousOrCurrent: all){		// The head of the queue is the element that has been on the queue the longest time.
			if (previousOrCurrent.rotated < rotatedNoEarlierThan){
				continue;
			}else if (previousOrCurrent.rotated >= rotatedBefore){
				break;
			}else{
				result.add(previousOrCurrent.obj);
			}
		}
		return result;
	}

	
	/**
	 * Purge those rotated before a specific time.
	 * @param postPurgeFunction		the function to be applied to the object after it has been purged. The function can be null if not necessary.
	 * @param rotatedBefore	milliseconds since 1970-01-01 UTC, exclusive
	 * 								This time should not be very close to current time, so that we can be sure there is no access to the object to be purged.
	 */
	public void purge(Consumer<T> postPurgeFunction, long rotatedBefore){
		while (true){
			Wrapper<T> previous = all.peek();
			if (previous.rotated > 0 && previous.rotated < rotatedBefore){
				all.remove(previous);
				if (postPurgeFunction != null){
					postPurgeFunction.accept(previous.obj);
				}
			}else{	// if reached the current one, or one rotated after the specified time
				break;
			}
		}
	}

}
