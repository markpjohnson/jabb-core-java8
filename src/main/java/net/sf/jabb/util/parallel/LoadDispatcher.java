package net.sf.jabb.util.parallel;

/**
 * A dispatcher that can dispatch work load and get result.
 * The implementation can be a direct dispatcher to queue service, a non-clustered load balancer, or a clustered load balancer.
 * @author James Hu
 *
 * @param <L>	Work load
 * @param <R>	Result
 */
public interface LoadDispatcher<L, R> {

	/**
	 * Dispatch the work load
	 * @param load	the work load
	 * @return	the result of the dispatching
	 */
	public abstract R dispatch(L load);

}