/**
 * 
 */
package net.sf.jabb.txsdp;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import net.sf.jabb.azure.AzureEventHubUtility;
import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.StreamDataSupplierWithIdAndRange;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.azure.AzureSequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;
import net.sf.jabb.util.parallel.WaitStrategies;

import org.junit.Test;

import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * @author James Hu
 *
 */
public class StreamDataBatchProcessingUsageTest {

public void createSuppliers() throws JMSException {
	
List<StreamDataSupplierWithId<String>> suppliersWithId = 
    AzureEventHubUtility.createStreamDataSuppliers(
        "your_name_space.servicebus.windows.net",
        "ReceiveRule",
        "the_key",
        "event_hub_name",
        AzureEventHubUtility.DEFAULT_CONSUMER_GROUP,
        message -> {
            try {
                BytesMessage msg = (BytesMessage) message;
                byte[] bytes = new byte[(int)msg.getBodyLength()];
                msg.readBytes(bytes, (int)msg.getBodyLength());
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (Exception e) {
                e.printStackTrace();
                return "<ERROR>";
            }
        });

}

public void specifyRanges(List<StreamDataSupplierWithId<String>> suppliersWithId){
	
Instant now = Instant.now();
List<StreamDataSupplierWithIdAndRange<String, ?>> suppliersWithIdAndRange = suppliersWithId.stream()
    .map(s->{
        try {
            s.getSupplier().start();
            StreamDataSupplierWithIdAndRange<String, ?> result = s.withRange(now.minus(Duration.ofMinutes(30)), null);
            return result;
        } catch (Exception e) {
            return null;
        }
    })
    .filter(s -> s != null)
    .collect(Collectors.toList());		

}

@Test
public void setOptions(){
	
TransactionalStreamDataBatchProcessing.Options options = 
	new TransactionalStreamDataBatchProcessing.Options()
	    .withInitialTransactionTimeoutDuration(Duration.ofMinutes(1))
	    .withMaxInProgressTransactions(10)
	    .withMaxRetringTransactions(10)
	    .withTransactionAcquisitionDelay(Duration.ofSeconds(30))
	    .withWaitStrategy(WaitStrategies.threadSleepStrategy());
	
}


public void initializeProcessing(TransactionalStreamDataBatchProcessing.Options options, SequentialTransactionsCoordinator txCoordinator, List<StreamDataSupplierWithIdAndRange<String, ?>> suppliersWithIdAndRange){
	
TransactionalStreamDataBatchProcessing<String> processing = 
	new TransactionalStreamDataBatchProcessing<>(
		"TestProcessing", 	// ID, useful in logging
		options, 			// options we specified
		txCoordinator, 		// instance of SequentialTransactionsCoordinator
	    (context, data) -> doYourProcessing(context, data),		// the actual batch processing
	    3000, 				// the maximum size of a batch
	    Duration.ofSeconds(60), 	// time to wait for fetching all the event in a retrying batch
	    Duration.ofSeconds(5),		// time to wait for fetching all the event in a new batch
	    suppliersWithIdAndRange);	// the streams

}

public void startStopEtc(int NUM_PROCESSORS, TransactionalStreamDataBatchProcessing<String> processing) throws TransactionStorageInfrastructureException, DataStreamInfrastructureException{
	
ExecutorService threadPool = Executors.newCachedThreadPool();
for (int i = 0; i < NUM_PROCESSORS; i ++){
    Runnable processor = processing.createProcessor(String.valueOf(i));
    threadPool.execute(processor);
}
processing.startAll();

TransactionalStreamDataBatchProcessing.Status status = processing.getStatus();

processing.stopAll();
threadPool.shutdown();
	
}

private boolean doYourProcessing(ProcessingContext context, ArrayList<String> data) {
	return false;
}

private SequentialTransactionsCoordinator createCoordinator() throws InvalidKeyException, URISyntaxException {
	
String connectionString = "your connection string";
CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
SequentialTransactionsCoordinator txCoordinator = new AzureSequentialTransactionsCoordinator(storageAccount, "your desired table name");
	
return txCoordinator;
}

	
}
