package net.sf.jabb.txsdp;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import net.sf.jabb.seqtx.SequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;

import org.slf4j.Logger;

/**
 * Internal implementation of ProcessingContext.
 * It is not thread safe.
 * @author James Hu
 *
 */
class ProcessingContextImpl implements ProcessingContext{
	private static final Logger logger = TransactionalStreamDataBatchProcessing.logger;
	
	SequentialTransactionsCoordinator txCoordinator;
	String seriesId;
	SequentialTransaction transaction;
	Map<String, Object> map;
	
	String previousTransactionEndPosition;
	boolean isOutOfRangeMessageReached;		// true if out of range message had reached which means probably we should stop processing
	boolean isOpenRangeSuccessfullyClosed;
	boolean isOpenRangeAbortedBecauseNothingReceived;
	
	
	ProcessingContextImpl(SequentialTransactionsCoordinator txCoordinator){
		this.txCoordinator = txCoordinator;
	}
	
	ProcessingContextImpl withSeriesId(String seriesId){
		this.seriesId = seriesId;
		return this;
	}
	
	ProcessingContextImpl withTransaction(SequentialTransaction transaction){
		this.transaction = transaction;
		this.isOutOfRangeMessageReached = false;
		this.isOpenRangeSuccessfullyClosed = false;
		this.isOpenRangeAbortedBecauseNothingReceived = false;
		return this;
	}
	
	@Override
	public Object put(String key, Object value){
		if (map == null){
			map = new HashMap<>();
		}
		return map.put(key, value);
	}
	
	@Override
	public Object get(String key){
		if (map == null){
			map = new HashMap<>();
		}
		return map.get(key);
	}
	
	@Override
	public boolean renewTransactionTimeout(Instant newTimeout) {
		try{
			txCoordinator.renewTransactionTimeout(seriesId, transaction.getProcessorId(), transaction.getTransactionId(), newTimeout);
			transaction.setTimeout(newTimeout);
			return true;
		}catch(Exception e){
			if (logger.isDebugEnabled()){
				logger.debug("Failed to renew transaction timeout for: seriesId={}, processorId={}, transactionId={}, startPosition={}, "
						+ "endPosition={}, timeout={}=>{}. Exception: {}",
					seriesId, transaction.getProcessorId(), transaction.getTransactionId(), transaction.getStartPosition(), 
					transaction.getEndPosition(), transaction.getTimeout(), newTimeout, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
			}
			return false;
		}
	}
	
	@Override
	public boolean updateTransactionDetail(Serializable newDetail) {
		try{
			txCoordinator.updateTransaction(seriesId, transaction.getProcessorId(), transaction.getTransactionId(), null, (Instant)null, newDetail);
			transaction.setDetail(newDetail);
			return true;
		}catch(Exception e){
			if (logger.isDebugEnabled()){
				logger.debug("Failed to update transaction detail for: seriesId={}, processorId={}, transactionId={}, startPosition={}, "
						+ "endPosition={}, timeout={}, detail={}=>{}. Exception: {}",
					seriesId, transaction.getProcessorId(), transaction.getTransactionId(), transaction.getStartPosition(), 
					transaction.getEndPosition(), transaction.getTimeout(), transaction.getDetail(), newDetail, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
			}
			return false;
		}
	}
	
	static class TransactionFinisherImpl implements TransactionFinisher{
		SequentialTransactionsCoordinator txCoordinator;
		String seriesId;
		String processorId;
		String transactionId;
		
		TransactionFinisherImpl(ProcessingContextImpl context){
			this.txCoordinator = context.txCoordinator;
			this.seriesId = context.seriesId;
			this.processorId = context.transaction.getProcessorId();
			this.transactionId = context.transaction.getTransactionId();
		}
		
		@Override
		public boolean finishTransaction() {
			try{
				txCoordinator.finishTransaction(seriesId, processorId, transactionId);
				return true;
			}catch(Exception e){
				if (logger.isDebugEnabled()){
					logger.debug("Unable to finish transaction for: seriesId={}, processorId={}, transactionId={}. Exception: {}",
						seriesId, processorId, transactionId, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
				}
				return false;
			}
		}

		@Override
		public boolean abortTransaction() {
			try{
				txCoordinator.abortTransaction(seriesId, processorId, transactionId);
				if (logger.isDebugEnabled()){
					logger.debug("Aborted transaction for: seriesId={}, processorId={}, transactionId={}.",
						seriesId, processorId, transactionId);
				}
				return true;
			}catch(Exception e){
				if (logger.isDebugEnabled()){
					logger.debug("Unable to abort transaction for: seriesId={}, processorId={}, transactionId={}. Exception: {}",
						seriesId, processorId, transactionId, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
				}
				return false;
			}
		}

		@Override
		public boolean renewTransactionTimeout(Instant newTimeout) {
			try{
				txCoordinator.renewTransactionTimeout(seriesId, processorId, transactionId, newTimeout);
				return true;
			}catch(Exception e){
				if (logger.isDebugEnabled()){
					logger.debug("Failed to renew transaction timeout for: seriesId={}, processorId={}, transactionId={}, newTimeout={}. Exception: {}",
						seriesId, processorId, transactionId, newTimeout, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
				}
				return false;
			}
		}
		
		@Override
		public boolean updateTransactionDetail(Serializable newDetail) {
			try{
				txCoordinator.updateTransaction(seriesId, processorId, transactionId, null, (Instant)null, newDetail);
				return true;
			}catch(Exception e){
				if (logger.isDebugEnabled()){
					logger.debug("Failed to update transaction detail for: seriesId={}, processorId={}, transactionId={}, newDetail={}. Exception: {}",
						seriesId, processorId, transactionId, newDetail, TransactionalStreamDataBatchProcessing.exceptionSummary(e));
				}
				return false;
			}
		}
		

	}
	
	@Override
	public TransactionFinisher getTransactionFinisher(){
		return new TransactionFinisherImpl(this);
	}

	@Override
	public Instant getTransactionTimeout() {
		return transaction.getTimeout();
	}

	@Override
	public String getTransactionSeriesId() {
		return seriesId;
	}

	@Override
	public String getProcessorId() {
		return transaction.getProcessorId();
	}

	@Override
	public String getTransactionId() {
		return transaction.getTransactionId();
	}
	
	@Override
	public String getTransactionStartPosition(){
		return transaction.getStartPosition();
	}
	
	@Override
	public String getTransactionEndPosition(){
		return transaction.getEndPosition();
	}

	@Override
	public Serializable getTransactionDetail() {
		return transaction.getDetail();
	}
	
	@Override
	public int getTransactionAttempts(){
		return transaction.getAttempts();
	}

}