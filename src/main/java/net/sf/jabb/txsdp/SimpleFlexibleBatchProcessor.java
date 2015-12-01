package net.sf.jabb.txsdp;

import java.time.Duration;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SimpleFlexibleBatchProcessor<T> implements FlexibleBatchProcessor<T>{
	static private final Logger logger = LoggerFactory.getLogger(SimpleFlexibleBatchProcessor.class);
	
	static final String KEY_DATA_ITEMS = SimpleFlexibleBatchProcessor.class.getSimpleName() + ".dataItems";
	static final String KEY_RECEIVE_TIMEOUT = SimpleFlexibleBatchProcessor.class.getSimpleName() + ".receiveTimeout";
	
	private SimpleBatchProcessor<T> simpleProcessor;
	private int maxBatchSize;
	private Duration receiveTimeout;
	private Duration receiveTimeoutForOpenRange;

	SimpleFlexibleBatchProcessor(){
	}
	
	SimpleFlexibleBatchProcessor(SimpleBatchProcessor<T> simpleProcessor, int maxBatchSize, Duration receiveTimeout, Duration receiveTimeoutForOpenRange){
		this();
		this.simpleProcessor = simpleProcessor;
		this.maxBatchSize = maxBatchSize;
		this.receiveTimeout = receiveTimeout;
		this.receiveTimeoutForOpenRange = receiveTimeoutForOpenRange;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean initialize(ProcessingContext context) {
		try{
			boolean isOpenRange = context.getTransactionEndPosition() == null;
			Long receiveShouldFinishTime = System.currentTimeMillis() + (isOpenRange ? receiveTimeoutForOpenRange.toMillis() : receiveTimeout.toMillis());

			context.put(KEY_RECEIVE_TIMEOUT, receiveShouldFinishTime);
			
			Object o = context.get(KEY_DATA_ITEMS);
			if (o == null || !(o instanceof ArrayList)){
				context.put(KEY_DATA_ITEMS, new ArrayList<T>(maxBatchSize));
			}else{
				((ArrayList<T>) o).clear();
			}
			return true;
		}catch(Exception e){
			logger.error("Failed to initialize", e);
			return false;
		}
	}

	@Override
	public long receive(ProcessingContext context, T dataItem) {
		if (dataItem != null){
			@SuppressWarnings("unchecked")
			ArrayList<T> dataItems = (ArrayList<T>) context.get(KEY_DATA_ITEMS);
			dataItems.add(dataItem);
			if (dataItems.size() >= maxBatchSize){
				return 0;
			}
		}
		return (Long)context.get(KEY_RECEIVE_TIMEOUT) - System.currentTimeMillis();
	}

	@Override
	public Boolean finish(ProcessingContext context) {
		@SuppressWarnings("unchecked")
		ArrayList<T> dataItems = (ArrayList<T>) context.get(KEY_DATA_ITEMS);
		try{
			return simpleProcessor.process(context, dataItems);
		}finally{
			dataItems.clear();
		}
	}
	
}