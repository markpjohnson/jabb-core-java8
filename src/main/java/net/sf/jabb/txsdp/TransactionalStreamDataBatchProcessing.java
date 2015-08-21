/**
 * 
 */
package net.sf.jabb.txsdp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.StreamDataSupplierWithIdAndRange;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.seqtx.ReadOnlySequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator.TransactionCounts;
import net.sf.jabb.seqtx.ex.DuplicatedTransactionIdException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.util.parallel.WaitStrategy;
import net.sf.jabb.util.text.DurationFormatter;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transactional batch processing of stream data.
 * 
 * @author James Hu
 * 
 * @param <M> type of the data/message/event
 *
 */
public class TransactionalStreamDataBatchProcessing<M> {
	static final Logger logger = LoggerFactory.getLogger(TransactionalStreamDataBatchProcessing.class);
	
	protected String id;
	protected FlexibleBatchProcessor<M> processor;
	protected List<StreamDataSupplierWithIdAndRange<M>> suppliers;
	protected SequentialTransactionsCoordinator txCoordinator;
	
	protected Options processorOptions;
	
	protected static int STATE_READY = 0;
	protected static int STATE_STOPPED = 1;
	protected static int STATE_PAUSED = 2;
	protected static int STATE_RUNNING = 3;
	protected AtomicInteger state = new AtomicInteger(STATE_READY);
	
	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			batch processor
	 * @param suppliers			stream data suppliers
	 */
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			FlexibleBatchProcessor<M> processor, List<StreamDataSupplierWithIdAndRange<M>> suppliers){
		this.id = id;
		this.processorOptions = new Options(processorOptions);
		this.txCoordinator = txCoordinator;
		this.processor = processor;
		this.suppliers = new ArrayList<>();
		this.suppliers.addAll(suppliers);
	}
	
	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			batch processor
	 * @param suppliers			stream data suppliers
	 */
	@SafeVarargs
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			FlexibleBatchProcessor<M> processor, StreamDataSupplierWithIdAndRange<M>... suppliers){
		this(id, processorOptions, txCoordinator, processor, Arrays.asList(suppliers));
	}
	
	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			simple batch processor
	 * @param maxBatchSize		maximum data items in a batch/transaction
	 * @param receiveTimeout	total duration allowed for receiving all the data items in a closed range batch/transaction
	 * @param receiveTimeoutForOpenRange	total duration allowed for receiving all the data items in a open range batch/transaction
	 * @param suppliers			stream data suppliers
	 */
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			SimpleBatchProcessor<M> processor, int maxBatchSize, Duration receiveTimeout, Duration receiveTimeoutForOpenRange,
			List<StreamDataSupplierWithIdAndRange<M>> suppliers){
		this(id, processorOptions, txCoordinator, 
				new SimpleFlexibleBatchProcessor<M>(processor, maxBatchSize, receiveTimeout, receiveTimeoutForOpenRange), 
				suppliers);
	}
	
	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			simple batch processor
	 * @param maxBatchSize		maximum data items in a batch/transaction
	 * @param receiveTimeout	total duration allowed for receiving all the data items in a closed range batch/transaction
	 * @param receiveTimeoutForOpenRange	total duration allowed for receiving all the data items in a open range batch/transaction
	 * @param suppliers			stream data suppliers
	 */
	@SafeVarargs
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			SimpleBatchProcessor<M> processor, int maxBatchSize, Duration receiveTimeout, Duration receiveTimeoutForOpenRange,
			StreamDataSupplierWithIdAndRange<M>... suppliers){
		this(id, processorOptions, txCoordinator, processor, maxBatchSize, receiveTimeout, receiveTimeoutForOpenRange, Arrays.asList(suppliers));
	}
	
	/**
	 * Create a runnable that does the processing.
	 * @param processorId	ID of the runnable
	 * @return	a runnable
	 */
	public Runnable createRunnalbe(String processorId){
		return new StreamDataProcessingRunnable(processorId);
	}
	
	/**
	 * Start processing. Once started, the processing can later be paused or stopped.
	 */
	public void start(){
		if (state.compareAndSet(STATE_READY, STATE_RUNNING)){
			return;
		}
		if (state.compareAndSet(STATE_PAUSED, STATE_RUNNING)){
			return;
		}
		throw new IllegalStateException("Cannot start when not in ready or paused state");
	}
	
	/**
	 * Pause processing. Once paused, the processing can later be started or stopped.
	 */
	public void pause(){
		if (state.compareAndSet(STATE_RUNNING, STATE_PAUSED)){
			return;
		}
		throw new IllegalStateException("Cannot pause when not in running state");
	}
	
	/**
	 * Stop processing. Once stopped, the processing cannot be restarted.
	 */
	public void stop(){
		state.set(STATE_STOPPED);
	}
	
	protected String seriesId(StreamDataSupplierWithIdAndRange<M> supplierWithIdAndRange){
		return id + "-" + supplierWithIdAndRange.getId();
	}

	class StreamDataProcessingRunnable implements Runnable{
		private String processorId;
		
		StreamDataProcessingRunnable(String processorId){
			this.processorId = processorId;
		}
		
		private void await(){
			WaitStrategy waitStrategy = processorOptions.getWaitStrategy();
			try{
				waitStrategy.await(processorOptions.getTransactionAcquisitionDelay().toMillis());
			}catch(InterruptedException ie){
				waitStrategy.handleInterruptedException(ie);
			}
		}
		
		@Override
		public void run() {
			// make sure we start from a random partition, and then do a round robin afterwards
			Random random = new Random(System.currentTimeMillis());
			int partition = random.nextInt(suppliers.size()) - 1;
			
			// reuse these data structures in the thread
			ProcessingContextImpl context = new ProcessingContextImpl(txCoordinator); 
			
			while(state.get() != STATE_STOPPED){
				while(state.get() == STATE_RUNNING){
					long startTime = System.currentTimeMillis();
					int attempts = 0;
					SequentialTransaction transaction = null;
					String seriesId = null;
					StreamDataSupplierWithIdAndRange<M> supplierWithIdAndRange;
					StreamDataSupplier<M> supplier = null;
					try{
						do{
							partition = (partition+1) % suppliers.size();
							supplierWithIdAndRange = suppliers.get(partition);
							supplier = supplierWithIdAndRange.getSupplier();
							attempts++;
							seriesId = seriesId(supplierWithIdAndRange);
							try {
								transaction = txCoordinator.startTransaction(seriesId, processorId, 
										processorOptions.getInitialTransactionTimeoutDuration(), 
										processorOptions.getMaxInProgressTransactions(), processorOptions.getMaxRetringTransactions());
							} catch (Exception e) {
								logger.warn("[{}] startTransaction(...) failed", seriesId, e);
								await();
							}
						}while(transaction == null && state.get() == STATE_RUNNING);
						
						// got a skeleton, with matching seriesId
						while (transaction != null && !transaction.hasStarted() &&  state.get() == STATE_RUNNING){
							String previousTransactionId = transaction.getTransactionId();
							String previousEndPosition = transaction.getStartPosition();
							transaction.setTransactionId(null);
							String startPosition = null;
							if (previousEndPosition == null){
								startPosition = supplierWithIdAndRange.getFromPosition();
								if (startPosition == null){
									startPosition = supplier.firstPosition();
								}
							}else{
								startPosition = supplier.nextStartPosition(previousEndPosition);
							}
							if (!supplier.isInRange(startPosition, supplierWithIdAndRange.getToPosition())){
								transaction = null;
								break;
							}
							transaction.setStartPosition(startPosition.toString());
							transaction.setEndPositionNull();	// for an open range transaction
							transaction.setTimeout(processorOptions.getInitialTransactionTimeoutDuration());
							attempts++;
							transaction = txCoordinator.startTransaction(seriesId, previousTransactionId, previousEndPosition, transaction, 
									processorOptions.getMaxInProgressTransactions(), processorOptions.getMaxRetringTransactions());
						}
					}catch(TransactionStorageInfrastructureException e){
						logger.debug("[{}] In transaction storage infrastructure error happened", seriesId, e);
					}catch(DuplicatedTransactionIdException e){
						logger.warn("[{}] Transaction ID is duplicated: " + transaction.getTransactionId(), seriesId, e);
					}catch(Exception e){
						logger.error("[{}] Error happened", seriesId, e);
					}
					
					if (transaction != null && transaction.hasStarted() && state.get() == STATE_RUNNING){
						logger.debug("Got a {} transaction {} [{}-{}] after {} attempts: {}", 
								(transaction.getAttempts() == 1 ? "new" : "failed"),
								transaction.getTransactionId(),
								transaction.getStartPosition(), transaction.getEndPosition(),
								attempts,
								DurationFormatter.formatSince(startTime));
						doTransaction(context.withSeriesId(seriesId).withTransaction(transaction), supplier);
					}else{
						if (state.get() == STATE_RUNNING){
							await();
						}
					}
				}  // jobState.get() == STATE_RUNNING
				await();
			} // jobState.get() != STATE_STOPPED
		}

		protected void doTransaction(ProcessingContextImpl context, StreamDataSupplier<M> supplier) {
			String seriesId = context.seriesId;
			SequentialTransaction transaction = context.transaction;
			
			boolean succeeded = false;
			String fetchedLastPosition = null;
			try{
				if (!processor.initialize(context)){
					throw new Exception("Unable to initilize processor");
				}
				long receiveTimeoutMillis = processor.receive(context, null);	// keep it for logging
				fetchedLastPosition = supplier.receive(msg->processor.receive(context, msg), transaction.getStartPosition(), transaction.getEndPosition());
				if (fetchedLastPosition != null){
					if (transaction.getEndPosition() == null){  // we need to close the open range
						try{
							txCoordinator.updateTransactionEndPosition(seriesId, processorId, transaction.getTransactionId(), fetchedLastPosition);
						}catch(Exception e){
							throw new Exception("Unable to update end position in open range transaction", e);
						}
					}else{  // we need to make sure that all items in the range had been fetched
						if (!fetchedLastPosition.equals(transaction.getEndPosition())){
							throw new Exception("Unable to fetch all the data in range within duration " + DurationFormatter.format(receiveTimeoutMillis));
						}
					}
					succeeded = processor.finish(context);
				}else{
					if (logger.isDebugEnabled()){
						logDebugInTransaction("Fetched nothing within " + DurationFormatter.format(receiveTimeoutMillis), context, fetchedLastPosition);
					}
				}
			}catch(Exception e){
				if (logger.isDebugEnabled()){
					logDebugInTransaction("Processing is not successful", context, fetchedLastPosition, e);
				}
			}
			if (succeeded){
				try{
					txCoordinator.finishTransaction(seriesId, processorId, transaction.getTransactionId(), fetchedLastPosition);
				}catch(Exception e){
					if (logger.isDebugEnabled()){
						logDebugInTransaction("Unable to finish transaction", context, fetchedLastPosition, e);
					}
				}
			}else{
				try{
					txCoordinator.abortTransaction(seriesId, processorId, transaction.getTransactionId());
					if (logger.isDebugEnabled()){
						logDebugInTransaction("Aborted transaction", context, fetchedLastPosition);
					}
				}catch(Exception e){
					if (logger.isDebugEnabled()){
						logDebugInTransaction("Unable to abort transaction", context, fetchedLastPosition, e);
					}
				}
			}
		}
		
		protected void logDebugInTransaction(String message, ProcessingContextImpl context, String fetchedLastPosition, Exception e){
			SequentialTransaction transaction = context.transaction;
			logger.debug("[{} - {}] " + message + ": transactionId={}, startPosition={}, endPosition={}, fetchedLastPosition={}. Exception: {}", 
					context.seriesId, processorId, transaction.getTransactionId(), transaction.getStartPosition(), transaction.getEndPosition(), 
					fetchedLastPosition, exceptionSummary(e));
		}
		
		protected void logDebugInTransaction(String message, ProcessingContextImpl context, String fetchedLastPosition){
			SequentialTransaction transaction = context.transaction;
			logger.debug("[{} - {}] " + message + ": transactionId={}, startPosition={}, endPosition={}, fetchedLastPosition={}", 
					context.seriesId, processorId, transaction.getTransactionId(), transaction.getStartPosition(), transaction.getEndPosition(), 
					fetchedLastPosition);
		}
	}
	
	static protected String exceptionSummary(final Throwable ex){
		Throwable e = ex;
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		sb.append(e.getClass().getName());
		sb.append(":").append(e.getMessage());
		sb.append(']');
		for(int i = 0; i < 5 && e.getCause() != null && e.getCause() != e; i ++){
			e = e.getCause();
			sb.append(" caused by [");
			sb.append(e.getClass().getName());
			sb.append(":").append(e.getMessage());
			sb.append(']');
		}
		return sb.toString();
	}
	
	/**
	 * Options for the processing.
	 * <ul>
	 * 	<li>maxBatchSize - the maximum number of data items to be fetched and processed in a batch. The actual numbers of data items
	 * 			in a batch will be smaller than or equal to this number.</li>
	 * 	<li>receiveTimeout - when fetching data for retrying a previously failed transaction which has a closed data range, 
	 * 			how long will the processors wait for all the data to be fetched. If the full range of data cannot be 
	 * 			fetched within this duration, the batch processing transaction will abort. Normally this duration should be
	 * 			long enough to make sure that data items as many as <code>maxBatchSize</code> can always be successfully fetched.</li>
	 * 	<li>receiveTimeoutForOpenRange - when fetching data for a new transaction which has an open data range 
	 * 			(from a specific time to the infinity), how long will the processors wait for a batch of data to be fetched.
	 * 			The longer this duration is, the more data will probably be fetched for a batch, and the larger the batch is and the
	 * 			longer the end-to-end processing delay is. Normally it should be shorter than <code>receiveTimeout</code></li>
	 * 	<li>initialTransactionTimeoutDuration - the initial timeout duration for the transactions. It should be long enough
	 * 			to allow data items for a batch to be fetched, Otherwise the processors may not have a chance to renew the transaction
	 * 			timeout before the transaction times out.</li>
	 * 	<li>maxInProgressTransactions - maximum number of transactions allowed to be in progress at the same time</li>
	 * 	<li>maxRetringTransactions - among in progress transactions, the maximum number of retrying transactions allowed at the same time</li>
	 * 	<li>transactionAcquisitionDelay - time to wait before next try to get a batch of data items for processing when 
	 * 			previously there was no data available for processing</li>
	 * 	<li>waitStrategy - the {@link WaitStrategy} specifying how to wait for a specific time duration</li>
	 * </ul>
	 * @author James Hu
	 *
	 */
	static public class Options{
		private Duration initialTransactionTimeoutDuration;
		private int maxInProgressTransactions;
		private int maxRetringTransactions;
		private Duration transactionAcquisitionDelay;
		private WaitStrategy waitStrategy;
		
		public Options(){
		}
		
		public Options(Options that){
			this.initialTransactionTimeoutDuration = that.initialTransactionTimeoutDuration;
			this.maxInProgressTransactions = that.maxInProgressTransactions;
			this.maxRetringTransactions = that.maxRetringTransactions;
			this.transactionAcquisitionDelay = that.transactionAcquisitionDelay;
			this.waitStrategy = that.waitStrategy;
		}
		
		public Duration getInitialTransactionTimeoutDuration() {
			return initialTransactionTimeoutDuration;
		}
		public void setInitialTransactionTimeoutDuration(Duration initialTransactionTimeoutDuration) {
			this.initialTransactionTimeoutDuration = initialTransactionTimeoutDuration;
		}
		public Options withInitialTransactionTimeoutDuration(Duration initialTransactionTimeoutDuration) {
			this.initialTransactionTimeoutDuration = initialTransactionTimeoutDuration;
			return this;
		}

		public int getMaxInProgressTransactions() {
			return maxInProgressTransactions;
		}
		public void setMaxInProgressTransactions(int maxInProgressTransactions) {
			this.maxInProgressTransactions = maxInProgressTransactions;
		}
		public Options withMaxInProgressTransactions(int maxInProgressTransactions) {
			this.maxInProgressTransactions = maxInProgressTransactions;
			return this;
		}
		
		public int getMaxRetringTransactions() {
			return maxRetringTransactions;
		}
		public void setMaxRetringTransactions(int maxRetringTransactions) {
			this.maxRetringTransactions = maxRetringTransactions;
		}
		public Options withMaxRetringTransactions(int maxRetringTransactions) {
			this.maxRetringTransactions = maxRetringTransactions;
			return this;
		}

		public Duration getTransactionAcquisitionDelay() {
			return transactionAcquisitionDelay;
		}
		public void setTransactionAcquisitionDelay(Duration transactionAcquisitionDelay) {
			this.transactionAcquisitionDelay = transactionAcquisitionDelay;
		}
		public Options withTransactionAcquisitionDelay(Duration transactionAcquisitionDelay) {
			this.transactionAcquisitionDelay = transactionAcquisitionDelay;
			return this;
		}

		public WaitStrategy getWaitStrategy() {
			return waitStrategy;
		}
		public void setWaitStrategy(WaitStrategy waitStrategy) {
			this.waitStrategy = waitStrategy;
		}
		public Options withWaitStrategy(WaitStrategy waitStrategy) {
			this.waitStrategy = waitStrategy;
			return this;
		}
	}
	
	public LinkedHashMap<String, Status> getStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException{
		LinkedHashMap<String, Status> result = new LinkedHashMap<>(suppliers.size());
		for (StreamDataSupplierWithIdAndRange<M> supplier: suppliers){
			String seriesId = seriesId(supplier);
			List<? extends ReadOnlySequentialTransaction> transactions = txCoordinator.getRecentTransactions(seriesId);
			
			TransactionCounts transactionCounts = SequentialTransactionsCoordinator.getTransactionCounts(transactions);
			String finishedPosition = SequentialTransactionsCoordinator.getFinishedPosition(transactions);
			String lastUnfinishedStartPosition = null;
			String lastUnfinishedEndPosition = null;
			Instant finishedEnqueuedTime = null;
			Instant lastUnfinishedStartEnqueuedTime = null;
			Instant lastUnfinishedEndEnqueuedTime = null;
			if (transactions != null && transactions.size() > 0){
				ReadOnlySequentialTransaction tx = transactions.get(transactions.size() - 1);
				if (tx.isInProgress()){
					lastUnfinishedStartPosition = tx.getStartPosition();
					lastUnfinishedEndPosition = tx.getEndPosition();
					if (lastUnfinishedStartPosition != null){
						lastUnfinishedStartEnqueuedTime = supplier.getSupplier().enqueuedTime(lastUnfinishedStartPosition);
					}
					if (lastUnfinishedEndPosition != null){
						lastUnfinishedEndEnqueuedTime = supplier.getSupplier().enqueuedTime(lastUnfinishedEndPosition);
					}
				}
			}
			if (finishedPosition != null){
				finishedEnqueuedTime = supplier.getSupplier().enqueuedTime(finishedPosition);
			}
			
			Status status = new Status();
			status.transactionCounts = transactionCounts;
			status.finishedPosition = finishedPosition;
			status.lastUnfinishedStartPosition = lastUnfinishedStartPosition;
			status.lastUnfinishedEndPosition = lastUnfinishedEndPosition;
			status.finishedEnqueuedTime = finishedEnqueuedTime;
			status.lastUnfinishedStartEnqueuedTime = lastUnfinishedStartEnqueuedTime;
			status.lastUnfinishedEndEnqueuedTime = lastUnfinishedEndEnqueuedTime;
			
			result.put(supplier.getId(), status);
		}
		return result;
	}
	
	/**
	 * Processing status for a single supplier with range.
	 * <ul>
	 * 	<li>finishedPosition - end position of the last transaction that is finished and all its successors are finished</li>
	 * 	<li>finishedEnqueuedTime - enqueued time of the message at finishedPosition</li>
	 * 	<li>lastUnfinishedStartPosition - start position of the last in progress transaction</li>
	 * 	<li>lastUnfinishedStartEnqueuedTime - enqueued time of the message at lastInProgressStartPosition</li>
	 * 	<li>lastUnfinishedEndPosition - end position of the last in progress transaction</li>
	 * 	<li>lastUnfinishedEndEnqueuedTime - enqueued time of the message at lastInProgressEndPosition</li>
	 * </ul>
	 * 
	 * @author James Hu
	 *
	 */
	public static class Status{
		private String finishedPosition;
		private Instant finishedEnqueuedTime;
		private String lastUnfinishedStartPosition;
		private Instant lastUnfinishedStartEnqueuedTime;
		private String lastUnfinishedEndPosition;
		private Instant lastUnfinishedEndEnqueuedTime;
		private TransactionCounts transactionCounts;
		
		Status(){
		}
		
		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

		/**
		 * @return the finishedPosition
		 */
		public String getFinishedPosition() {
			return finishedPosition;
		}

		/**
		 * @return the finishedEnqueuedTime
		 */
		public Instant getFinishedEnqueuedTime() {
			return finishedEnqueuedTime;
		}

		/**
		 * @return the lastUnfinishedStartPosition
		 */
		public String getLastUnfinishedStartPosition() {
			return lastUnfinishedStartPosition;
		}

		/**
		 * @return the lastUnfinishedStartEnqueuedTime
		 */
		public Instant getLastUnfinishedStartEnqueuedTime() {
			return lastUnfinishedStartEnqueuedTime;
		}

		/**
		 * @return the lastUnfinishedEndPosition
		 */
		public String getLastUnfinishedEndPosition() {
			return lastUnfinishedEndPosition;
		}

		/**
		 * @return the lastUnfinishedEndEnqueuedTime
		 */
		public Instant getLastUnfinishedEndEnqueuedTime() {
			return lastUnfinishedEndEnqueuedTime;
		}

		/**
		 * @return the transactionCounts
		 */
		public TransactionCounts getTransactionCounts() {
			return transactionCounts;
		}
		
		
	}

}
