/**
 * Created by mjohnson on 2/29/2016.
 */
package net.sf.jabb.txsdp;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;

import java.util.LinkedHashMap;
import java.util.Map;

public interface StreamDataProcessing<M> {
	Runnable createProcessor(String processorId);
	
	void start(String processorId);
	
	void pause(String processorId);
	
	void stop(String processorId);
	
	void startAll();
	
	void pauseAll();
	
	void stopAll();
	
	TransactionalStreamDataBatchProcessing.Status getStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException;
	
	Map<String, TransactionalStreamDataBatchProcessing.ProcessorStatus> getProcessorStatus();
	
	LinkedHashMap<String, TransactionalStreamDataBatchProcessing.StreamStatus> getStreamStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException;
}
