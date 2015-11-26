/**
 * 
 */
package net.sf.jabb.txsdp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.StreamDataSupplierWithIdAndPositionRange;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
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
	protected FlexibleBatchProcessor<M> batchProcessor;
	protected List<StreamDataSupplierWithIdAndRange<M, ?>> suppliers;
	protected SequentialTransactionsCoordinator txCoordinator;
	
	protected Options processorOptions;
	
	public static enum State{
		READY, 		// just initialized
		STOPPED, 	// stopped, run() method exited
		PAUSED, 	// paused, run() method is still executing, waiting for the processing to be resumed
		RUNNING, 	// running
		FINISHED, 	// all the data within the range had been processed, run() method exited
		STOPPING, 	// will be stopped soon, run() method is still executing
		PAUSING;	// will be paused soon, run() method is still executing
	}
	
	protected Map<String, Processor> processors = new HashMap<>();
	
	public List<StreamDataSupplierWithIdAndRange<M, ?>> getSuppliers() {
		return suppliers;
	}

	/**
	 * Set or change the suppliers. Changing suppliers while processors are running is allowed.
	 * Processors are able to detect the change and start working with the new suppliers.
	 * @param suppliers the suppliers to set
	 */
	public void setSuppliers(List<StreamDataSupplierWithIdAndRange<M, ?>> suppliers) {
		this.suppliers = suppliers;
	}

	/**
	 * Get the transaction coordinator
	 * @return the txCoordinator
	 */
	public SequentialTransactionsCoordinator getTransactionCoordinator() {
		return txCoordinator;
	}

	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			batch processor
	 * @param suppliers			stream data suppliers
	 */
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			FlexibleBatchProcessor<M> processor, List<StreamDataSupplierWithIdAndRange<M, ?>> suppliers){
		this.id = id;
		this.processorOptions = new Options(processorOptions);
		this.txCoordinator = txCoordinator;
		this.batchProcessor = processor;
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
			FlexibleBatchProcessor<M> processor, StreamDataSupplierWithIdAndRange<M, ?>... suppliers){
		this(id, processorOptions, txCoordinator, processor, Arrays.asList(suppliers));
	}
	
	/**
	 * Constructor
	 * @param id				ID of this processing
	 * @param processorOptions	options
	 * @param txCoordinator		transactions coordinator
	 * @param processor			simple batch processor
	 * @param maxBatchSize		the maximum number of data items to be fetched and processed in a batch. 
	 * 							The actual numbers of data items in a batch will be smaller than or equal to this number.
	 * @param receiveTimeout	total duration allowed for receiving all the data items in a closed range batch/transaction.
	 * 							When fetching data for retrying a previously failed transaction which has a closed data range, 
	 * 							this argument decides how long the processors wait for all the data to be fetched. 
	 * 							If the full range of data cannot be fetched within this duration, 
	 * 							the batch processing transaction will abort. Normally this duration should be long enough to make 
	 * 							sure that data items as many as <code>maxBatchSize</code> can always be successfully fetched.
	 * @param receiveTimeoutForOpenRange	total duration allowed for receiving all the data items in a open range batch/transaction.
	 * 							When fetching data for a new transaction which has an open data range (from a specific time to the infinity), 
	 * 							this argument decides how long the processors wait for a batch of data to be fetched.
	 * 							The longer this duration is, the more data will probably be fetched for a batch, and the larger the 
	 * 							batch is and the longer the end-to-end processing delay is. 
	 * 							Normally it should be shorter than <code>receiveTimeout</code>
	 * @param suppliers			stream data suppliers
	 */
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			SimpleBatchProcessor<M> processor, int maxBatchSize, Duration receiveTimeout, Duration receiveTimeoutForOpenRange,
			List<StreamDataSupplierWithIdAndRange<M, ?>> suppliers){
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
	 * @param maxBatchSize		the maximum number of data items to be fetched and processed in a batch. 
	 * 							The actual numbers of data items in a batch will be smaller than or equal to this number.
	 * @param receiveTimeout	total duration allowed for receiving all the data items in a closed range batch/transaction.
	 * 							When fetching data for retrying a previously failed transaction which has a closed data range, 
	 * 							this argument decides how long the processors wait for all the data to be fetched. 
	 * 							If the full range of data cannot be fetched within this duration, 
	 * 							the batch processing transaction will abort. Normally this duration should be long enough to make 
	 * 							sure that data items as many as <code>maxBatchSize</code> can always be successfully fetched.
	 * @param receiveTimeoutForOpenRange	total duration allowed for receiving all the data items in a open range batch/transaction.
	 * 							When fetching data for a new transaction which has an open data range (from a specific time to the infinity), 
	 * 							this argument decides how long the processors wait for a batch of data to be fetched.
	 * 							The longer this duration is, the more data will probably be fetched for a batch, and the larger the 
	 * 							batch is and the longer the end-to-end processing delay is. 
	 * 							Normally it should be shorter than <code>receiveTimeout</code>
	 * @param suppliers			stream data suppliers
	 */
	@SafeVarargs
	public TransactionalStreamDataBatchProcessing(String id, Options processorOptions, SequentialTransactionsCoordinator txCoordinator, 
			SimpleBatchProcessor<M> processor, int maxBatchSize, Duration receiveTimeout, Duration receiveTimeoutForOpenRange,
			StreamDataSupplierWithIdAndPositionRange<M>... suppliers){
		this(id, processorOptions, txCoordinator, processor, maxBatchSize, receiveTimeout, receiveTimeoutForOpenRange, Arrays.asList(suppliers));
	}
	
	/**
	 * Create a processor that does the processing.
	 * @param processorId	ID of the processor
	 * @return	a runnable processor that can be run in any thread
	 */
	public Runnable createProcessor(String processorId){
		Validate.notNull(processorId, "Processor id cannot be null");
		if (processors.containsKey(processorId)){
			throw new IllegalArgumentException("Another runnable with the same processor ID already exists: " + processorId);
		}
		Processor runnable = new Processor(processorId);
		processors.put(processorId, runnable);
		return runnable;
	}
	
	/**
	 * Start processing. Once started, the processing can later be paused or stopped.
	 * @param runnable the runnable to be started
	 */
	protected void start(Processor runnable){
		if (runnable.state.compareAndSet(State.READY, State.RUNNING)){
			return;
		}
		if (runnable.state.compareAndSet(State.PAUSED, State.RUNNING)){
			return;
		}
		throw new IllegalStateException("Cannot start when in " + runnable.state.get() + " state: " + runnable.processorId);
	}
	
	/**
	 * Pause processing. Once paused, the processing can later be started or stopped.
	 * @param runnable the runnable to be paused
	 */
	protected void pause(Processor runnable){
		if (runnable.state.compareAndSet(State.RUNNING, State.PAUSING)){
			return;
		}
		throw new IllegalStateException("Cannot pause when not in running state: " + runnable.processorId);
	}
	
	/**
	 * Stop processing. Once stopped, the processing cannot be restarted.
	 * @param runnable the runnable to be stopped
	 */
	protected void stop(Processor runnable){
		if (runnable.state.compareAndSet(State.RUNNING, State.STOPPING)){
			return;
		}
		if (runnable.state.compareAndSet(State.PAUSED, State.STOPPING)){
			return;
		}
		if (runnable.state.compareAndSet(State.PAUSING, State.STOPPING)){
			return;
		}
	}

	/**
	 * Start processing. Once started, the processing can later be paused or stopped.
	 * @param processorId ID of the processor to be started
	 */
	public void start(String processorId){
		Processor runnable = processors.get(processorId);
		Validate.notNull(runnable, "There is no processor with the id: " + processorId);

		start(runnable);
	}
	
	/**
	 * Pause processing. Once paused, the processing can later be started or stopped.
	 * @param processorId ID of the processor to be paused
	 */
	public void pause(String processorId){
		Processor runnable = processors.get(processorId);
		Validate.notNull(runnable, "There is no processor with the id: " + processorId);

		pause(runnable);
	}
	
	/**
	 * Stop processing. Once stopped, the processing cannot be restarted.
	 * @param processorId ID of the processor to be stopped
	 */
	public void stop(String processorId){
		Processor runnable = processors.get(processorId);
		Validate.notNull(runnable, "There is no processor with the id: " + processorId);

		stop(runnable);
	}

	/**
	 * Start processing. Once started, the processing can later be paused or stopped.
	 */
	public void startAll(){
		for (Processor runnable: processors.values()){
			start(runnable);
		}
	}
	
	/**
	 * Pause processing. Once paused, the processing can later be started or stopped.
	 */
	public void pauseAll(){
		for (Processor runnable: processors.values()){
			pause(runnable);
		}
	}
	
	/**
	 * Stop processing. Once stopped, the processing cannot be restarted.
	 */
	public void stopAll(){
		for (Processor runnable: processors.values()){
			stop(runnable);
		}
	}
	
	protected String seriesId(StreamDataSupplierWithIdAndRange<M, ?> supplierWithId){
		return id + "_" + supplierWithId.getId().replace('/', '_');
	}
	
	class Processor implements Runnable{
		protected AtomicReference<State> state = new AtomicReference<>(State.READY);
		private String processorId;
		
		Processor(String processorId){
			this.processorId = processorId;
		}
		
		private void await(){
			long millis = processorOptions.getTransactionAcquisitionDelay().toMillis();
			if (millis > 0){
				WaitStrategy waitStrategy = processorOptions.getWaitStrategy();
				try{
					waitStrategy.await(millis);
				}catch(InterruptedException ie){
					waitStrategy.handleInterruptedException(ie);
				}
			}
		}
		
		private boolean allProcessed(boolean[] outOfRangeReached){
			for (boolean b: outOfRangeReached){
				if (!b){
					return false;
				}
			}
			return true;
		}
		
		@Override
		public void run() {
			logger.debug("[{}] Start running: {}", processorId, state);
			
			List<StreamDataSupplierWithIdAndRange<M, ?>> localSuppliers = new ArrayList<>(suppliers.size());
			localSuppliers.addAll(suppliers);

			boolean[] outOfRangeReached = new boolean[localSuppliers.size()];
			// make sure we start from a random partition, and then do a round robin afterwards
			Random random = new Random();
			int partition = random.nextInt(outOfRangeReached.length);
			
			// reuse these data structures in the thread
			ProcessingContextImpl context = new ProcessingContextImpl(txCoordinator); 
			
			while(!state.compareAndSet(State.STOPPING, State.STOPPED)){
				if (!localSuppliers.equals(suppliers)){	// if suppliers changed
					localSuppliers.clear();
					localSuppliers.addAll(suppliers);
					outOfRangeReached = new boolean[localSuppliers.size()];
					partition = partition % outOfRangeReached.length;
				}
				
				while(state.get() == State.RUNNING){
					if (allProcessed(outOfRangeReached)){
						state.set(State.FINISHED);
						break;
					}
					
					long startTime = System.currentTimeMillis();
					int attempts = 0;
					SequentialTransaction transaction = null;
					String seriesId = null;
					StreamDataSupplierWithIdAndRange<M, ?> supplierWithIdAndRange = null;
					StreamDataSupplier<M> supplier = null;
					try{
						while (state.get() == State.RUNNING && !outOfRangeReached[partition]){
							supplierWithIdAndRange = localSuppliers.get(partition);
							supplier = supplierWithIdAndRange.getSupplier();
							seriesId = seriesId(supplierWithIdAndRange);
							if (context.isOpenRangeSuccessfullyClosed){
								transaction = context.transaction;	// assume that last transaction just finished is the globally last transaction
								break;
							}
							attempts++;
							try {
								transaction = txCoordinator.startTransaction(seriesId, processorId, 
										processorOptions.getInitialTransactionTimeoutDuration(), 
										processorOptions.getMaxInProgressTransactions(), processorOptions.getMaxRetringTransactions());
							} catch (Exception e) {
								logger.warn("[{}] startTransaction(...) failed", seriesId, e);
							}
							if (transaction != null){
								break;
							}
							await();
							partition = (partition+1) % outOfRangeReached.length;
						}
						
						// got a skeleton, with matching seriesId
						while (transaction != null && (!transaction.hasStarted() || context.isOpenRangeSuccessfullyClosed) &&  state.get() == State.RUNNING){
							String previousTransactionId = transaction.getTransactionId();
							String previousEndPosition;
							if (context.isOpenRangeSuccessfullyClosed){
								context.isOpenRangeSuccessfullyClosed = false;	// can only be used once
								previousEndPosition = transaction.getEndPosition();
							}else{
								previousEndPosition = transaction.getStartPosition();
							}
							transaction.setTransactionId(null);
							
							String startPosition = null;
							if (previousEndPosition == null){
								startPosition = "";	// the beginning of the range
							}else{
								startPosition = supplier.nextStartPosition(previousEndPosition);
							}
							transaction.setStartPosition(startPosition);
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
					
					if (transaction != null && transaction.hasStarted() && state.get() == State.RUNNING){
						logger.debug("Got a {} transaction {} [{}-{}] after {} attempts: {}", 
								(transaction.getAttempts() == 1 ? "new" : "failed"),
								transaction.getTransactionId(),
								transaction.getStartPosition(), transaction.getEndPosition(),
								attempts,
								DurationFormatter.formatSince(startTime));
						doTransaction(context.withSeriesId(seriesId).withTransaction(transaction), supplierWithIdAndRange);
						if (context.isOutOfRangeMessageReached){
							String finishedPosition;
							try {
								finishedPosition = SequentialTransactionsCoordinator.getFinishedPosition(txCoordinator.getRecentTransactions(seriesId));
								if (finishedPosition != null && 
										(finishedPosition.equals(transaction.getStartPosition()) || finishedPosition.equals(transaction.getEndPosition()))){
									outOfRangeReached[partition] = true;
								}
							} catch (Exception e) {
								logger.warn("[{}] Failed to get recent transactions", seriesId, e);
							}
						}
					}else{ // can't get a transaction
						if (state.get() == State.RUNNING && !outOfRangeReached[partition]){
							await();
						}
					}
				}  // state.get() == State.RUNNING
				state.compareAndSet(State.PAUSING, State.PAUSED);
				if (allProcessed(outOfRangeReached)){
					state.set(State.FINISHED);
					break;
				}
			} // state.compareAndSet(State.STOPPING, State.STOPPED)
			
			logger.debug("[{}] Finish running: {}", processorId, state);
		}
		

		/**
		 * perform a batch processing transaction
		 * @param context	the processing context which will be updated in this method
		 * @param supplierWithIdAndRange	the stream data supplier
		 */
		protected void doTransaction(ProcessingContextImpl context, StreamDataSupplierWithIdAndRange<M, ?> supplierWithIdAndRange) {
			String seriesId = context.seriesId;
			SequentialTransaction transaction = context.transaction;
			
			boolean succeeded = false;
			String fetchedLastPosition = null;
			ReceiveStatus receiveStatus = null;
			
			boolean isInitiallyOpenRange = transaction.getEndPosition() == null;
			boolean isOpenRangeClosed = false;
			boolean isSuccessfullyFinished = false;
			try{
				if (!batchProcessor.initialize(context)){
					throw new Exception("Unable to initilize processor");
				}
				long receiveTimeoutMillis = batchProcessor.receive(context, null);	// keep it for logging
				receiveStatus = supplierWithIdAndRange.receiveInRange(msg->batchProcessor.receive(context, msg), transaction.getStartPosition());
				fetchedLastPosition = receiveStatus.getLastPosition();
				if (fetchedLastPosition != null){
					if (isInitiallyOpenRange){  // we need to close the open range
						try{
							txCoordinator.updateTransactionEndPosition(seriesId, processorId, transaction.getTransactionId(), fetchedLastPosition);
							transaction.setEndPosition(fetchedLastPosition);
							isOpenRangeClosed = true;
						}catch(Exception e){
							throw new Exception("Unable to update end position in open range transaction", e);
						}
					}else{  // we need to make sure that all items in the range had been fetched
						if (!fetchedLastPosition.equals(transaction.getEndPosition())){
							throw new Exception("Unable to fetch all the data in range within duration " + DurationFormatter.format(receiveTimeoutMillis));
						}
					}
					succeeded = batchProcessor.finish(context);
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
					isSuccessfullyFinished = true;
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
			
			context.isOutOfRangeMessageReached = receiveStatus == null ? false : receiveStatus.isOutOfRangeReached();
			context.isOpenRangeSuccessfullyClosed = isInitiallyOpenRange && isOpenRangeClosed && isSuccessfullyFinished;
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
	 * Get the overall status of the processing
	 * @return	overall status
	 * @throws TransactionStorageInfrastructureException		any exception happened in transaction storage
	 * @throws DataStreamInfrastructureException				any exception happened in data stream 
	 */
	public Status getStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException{
		Status status = new Status();
		status.processorStatus = getProcessorStatus();
		status.streamStatus = getStreamStatus();
		return status;
	}
	
	/**
	 * Get the status of the processors
	 * @return	status of the processors, key-ed by IDs of the processors in alphabet order
	 */
	public Map<String, ProcessorStatus> getProcessorStatus(){
		Map<String, ProcessorStatus> result = new TreeMap<>();
		for (Processor runnable: processors.values()){
			ProcessorStatus status = new ProcessorStatus();
			status.state = runnable.state.get();
			result.put(runnable.processorId, status);
		}
		return result;
	}
	
	/**
	 * Get processing status per stream
	 * @return	stream processing status per stream listed in the original order of those streams, key-ed by IDs of the streams
	 * @throws TransactionStorageInfrastructureException		any exception happened in transaction storage
	 * @throws DataStreamInfrastructureException				any exception happened in data stream 
	 */
	public LinkedHashMap<String, StreamStatus> getStreamStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException{
		List<StreamDataSupplierWithIdAndRange<M, ?>> localSuppliers = new ArrayList<>(suppliers.size());
		localSuppliers.addAll(suppliers);

		LinkedHashMap<String, StreamStatus> result = new LinkedHashMap<>(localSuppliers.size());
		for (StreamDataSupplierWithIdAndRange<M, ?> supplier: localSuppliers){
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
					if (StringUtils.isNotBlank(lastUnfinishedStartPosition)){
						lastUnfinishedStartEnqueuedTime = supplier.getSupplier().enqueuedTime(lastUnfinishedStartPosition);
					}
					if (StringUtils.isNotBlank(lastUnfinishedEndPosition)){
						lastUnfinishedEndEnqueuedTime = supplier.getSupplier().enqueuedTime(lastUnfinishedEndPosition);
					}
				}
			}
			if (finishedPosition != null){
				finishedEnqueuedTime = supplier.getSupplier().enqueuedTime(finishedPosition);
			}
			
			StreamStatus status = new StreamStatus();
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
	 * Processing status for a single stream with range.
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
	public static class StreamStatus{
		private String finishedPosition;
		private Instant finishedEnqueuedTime;
		private String lastUnfinishedStartPosition;
		private Instant lastUnfinishedStartEnqueuedTime;
		private String lastUnfinishedEndPosition;
		private Instant lastUnfinishedEndEnqueuedTime;
		private TransactionCounts transactionCounts;
		
		StreamStatus(){
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
	
	/**
	 * Status of a single processor
	 * @author James Hu
	 *
	 */
	public static class ProcessorStatus{
		private State state;

		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
		
		/**
		 * @return the state
		 */
		public State getState() {
			return state;
		}
	}

	/**
	 * Overall status of the processing
	 * @author James Hu
	 *
	 */
	public static class Status{
		private Map<String, ProcessorStatus> processorStatus;
		private LinkedHashMap<String, StreamStatus> streamStatus;
		
		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
		
		/**
		 * @return the processorStatus
		 */
		public Map<String, ProcessorStatus> getProcessorStatus() {
			return processorStatus;
		}
		/**
		 * @return the streamStatus
		 */
		public LinkedHashMap<String, StreamStatus> getStreamStatus() {
			return streamStatus;
		}
	}
	
	/**
	 * Options for the processing.
	 * <ul>
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
	
}
