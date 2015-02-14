/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.util.concurrent.Future;

/**
 * A pipeline is a set of data processing elements connected in series, 
 * where the output of one element is the input of the next one. 
 * The elements of a pipeline are often executed in parallel.
 * 
 * @author James Hu
 *
 */
public interface Pipeline<I, O> {
	/**
	 * Feed an input, and get a future of the final result in return.
	 * @param input	the input
	 * @return	future of the final result.
	 */
	Future<O> feed(I input);
	
	/**
	 * The intermediate output between two connected pipelines.
	 * @author James
	 *
	 * @param <O1>	Type of the output from the upstream pipeline
	 * @param <O2>	Type of the output from the downstream pipeline
	 */
	public static class IntermediateOutput<O1, O2>{
		O1 outputFromUpstream;
		Future<O2> futureFromDownstream;
		
		IntermediateOutput(O1 output, Future<O2> future){
			this.outputFromUpstream = output;
			this.futureFromDownstream = future;
		}
		
		public O1 getOutputFromUpstream() {
			return outputFromUpstream;
		}
		public Future<O2> getFutureFromDownstream() {
			return futureFromDownstream;
		}
		
	}
}
