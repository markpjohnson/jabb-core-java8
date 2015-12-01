package net.sf.jabb.txsdp;


/**
 * A flexible batch processor that supports real-time receiving of data items
 * @author James Hu
 * @param <T> type of the data item/message
 */
public interface FlexibleBatchProcessor<T> {
	/**
	 * Initialize for a batch processing
	 * @param context	the context of the batch
	 * @return  true if successfully initialized for the processing of the batch, false if the initialization failed
	 */
	boolean initialize(ProcessingContext context);
	
	/**
	 * Receive a single data item/message
	 * @param context	the context of the batch
	 * @param dataItem	a single item/message, it can be null which should be ignored
	 * @return	number of milliseconds left for receiving remaining items/messages, can be zero or negative meaning should stop receiving
	 */
	long receive(ProcessingContext context, T dataItem);
	
	/**
	 * Finish current batch
	 * @param context	the context of the batch
	 * @return	true if the batch finished successfully, false if unsuccessful, 
	 * null if the transaction will be finished or aborted by the FlexibleBatchProcessor itself.
	 */
	Boolean finish(ProcessingContext context);
}