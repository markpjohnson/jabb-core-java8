package net.sf.jabb.txsdp;

import java.util.ArrayList;

/**
 * Equivalent to Azure <code>IEventProcessor</code> and AWS <code>IRecordProcessor</code>
 * @author James Hu
 * @param <T> type of the data item/message
 */
@FunctionalInterface
public interface SimpleBatchProcessor<T> {

	/**
	 * Process a batch of data.
	 * @param context	the context of this batch/transaction
	 * @param data		List of all the data items, can be empty, never be null. It is of ArrayList type for better performance.
	 * @return		true if the batch finished successfully, false otherwise.
	 */
	boolean process(ProcessingContext context, ArrayList<T> data);
}