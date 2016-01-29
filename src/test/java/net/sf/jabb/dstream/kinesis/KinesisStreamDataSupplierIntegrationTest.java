/**
 * 
 */
package net.sf.jabb.dstream.kinesis;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Tests for KinesisStreamDataSupplier. It requires environment variable to be configured.
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KinesisStreamDataSupplierIntegrationTest {
	static String awsAccessKeyId, awsSecretKey, endpoint, streamName;
	static long pollInterval = 1000;
	static int fetchBatchSize = 200;
	static int receiveBatchSize = 10;
	
	@BeforeClass
	public static void readConfigurations(){
		awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
		awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
		endpoint = System.getenv("AWS_KINESIS_ENDPOINT");
		streamName = System.getenv("AWS_KINESIS_STREAM_NAME");
	}
	

	@Test
	public void test01Creation() throws DataStreamInfrastructureException {
		List<StreamDataSupplierWithId<UserRecord>> suppliersWithId = KinesisStreamDataSupplier.create(
				awsAccessKeyId, awsSecretKey, endpoint, streamName, x->x, 
				pollInterval, fetchBatchSize, receiveBatchSize);
		assertNotNull(suppliersWithId);
		assertTrue(suppliersWithId.size() > 0);
		System.out.println(suppliersWithId);
		
		for (StreamDataSupplierWithId<UserRecord> supplierWithId: suppliersWithId){
			StreamDataSupplier<UserRecord> supplier = supplierWithId.getSupplier();
			String lastPosition;
			Instant lastEnqueuedTime = null;
			try{
				lastPosition = supplier.lastPosition();
				lastEnqueuedTime = lastPosition == null ? null : supplier.enqueuedTime(lastPosition);
			}catch(DataStreamInfrastructureException e){
				e.printStackTrace();
				lastPosition = "Unknown";
			}
			
			System.out.println(supplierWithId.getId() + ": " + supplier + "\t-> " + lastPosition + " (" + lastEnqueuedTime + ")");
		}
	}
	
	@Test
	public void test02Decode() throws DataStreamInfrastructureException, InterruptedException {
		List<StreamDataSupplierWithId<String>> suppliersWithId = KinesisStreamDataSupplier.create(
				awsAccessKeyId, awsSecretKey, endpoint, streamName, r->new String(r.getData().array(), StandardCharsets.UTF_8), 
				pollInterval, fetchBatchSize, receiveBatchSize);
		StreamDataSupplier<String> supplier = suppliersWithId.get(0).getSupplier();
		
		// try to fetch first 100 user records
		List<String> records = new ArrayList<>();
		ReceiveStatus status = supplier.fetch(records, 
				supplier.firstPosition(), 
				100, Duration.ofSeconds(10));
		System.out.println(status);
		for (String r: records){
			System.out.println(r);
		}
		assertTrue(records.size() <= 100);
		assertEquals(100, records.size());
		String lastPositionInShard = null;
		try{
			lastPositionInShard = supplier.lastPosition();
		}catch(DataStreamInfrastructureException e){
			e.printStackTrace();
		}
		assertTrue(supplier.isInRange(status.getLastPosition(), lastPositionInShard));
	}
	
	@Test
	public void test03FetchAndReceive() throws DataStreamInfrastructureException, InterruptedException {
		List<StreamDataSupplierWithId<UserRecord>> suppliersWithId = KinesisStreamDataSupplier.create(
				awsAccessKeyId, awsSecretKey, endpoint, streamName, x->x, 
				pollInterval, fetchBatchSize, receiveBatchSize);
		StreamDataSupplier<UserRecord> supplier = suppliersWithId.get(0).getSupplier();
		
		// try to fetch first 100 user records
		List<UserRecord> records = new ArrayList<>();
		ReceiveStatus status = supplier.fetch(records, 
				supplier.firstPosition(), 
				100, Duration.ofSeconds(10));
		System.out.println(status);
		for (Record r: records){
			System.out.println(r);
		}
		assertTrue(records.size() <= 100);
		assertEquals(100, records.size());
		
		// try to advance to somewhere far above trim_horizon
		records.clear();
		status = supplier.fetch(records, 
				status.getLastPosition(),
				Instant.now().minus(Duration.ofHours(22)),
				300000, Duration.ofMinutes(10));

		String startPosition = status.getLastPosition();
		System.out.println("Start position: " + startPosition);
		
		// fetch in one go
		records.clear();
		status = supplier.fetch(records, startPosition, 510, Duration.ofSeconds(30));
		Set<String> ids = new TreeSet<>();
		ids.addAll(collectIds(records));
		
		// fetch in batches
		records.clear();
		status = supplier.fetch(records, startPosition, 100, Duration.ofSeconds(10));
		Set<String> ids2 = new TreeSet<>();
		ids2.addAll(collectIds(records));

		records.clear();
		status = supplier.fetch(records, status.getLastPosition(), 200, Duration.ofSeconds(10));
		ids2.addAll(collectIds(records));
		
		records.clear();
		status = supplier.fetch(records, status.getLastPosition(), 100, Duration.ofSeconds(10));
		ids2.addAll(collectIds(records));
		
		records.clear();
		status = supplier.fetch(records, status.getLastPosition(), 110, Duration.ofSeconds(10));
		ids2.addAll(collectIds(records));
		
		assertEquals(ids.size(), ids2.size());
		Object[] idsArray = ids.stream().toArray();
		Object[] ids2Array = ids2.stream().toArray();
		for (int i = 0; i < idsArray.length; i ++){
			if (!idsArray[i].equals(ids2Array[i])){
				System.out.println("Not the same at " + i + ":\t" + idsArray[i] + "\t" + ids2Array[i]);
			}
		}
		assertEquals(ids, ids2);
		
		// receive in batches
		Set<String> ids3 = new TreeSet<>();
		AtomicInteger count = new AtomicInteger();
		status = supplier.receive(r->{
			if (r == null){
				return 60000L;
			}else{
				ids3.add(r.getSequenceNumber() + "-" + r.getSubSequenceNumber());
				if (count.incrementAndGet() < 200){
					return 60000L;
				}else{
					return -1L;
				}
			}
		}, startPosition, Instant.now());
		assertEquals(200, count.get());
		
		count.set(0);
		status = supplier.receive(r->{
			if (r == null){
				return 60000L;
			}else{
				ids3.add(r.getSequenceNumber() + "-" + r.getSubSequenceNumber());
				if (count.incrementAndGet() < 100){
					return 60000L;
				}else{
					return -1L;
				}
			}
		}, status.getLastPosition(), Instant.now());
		assertEquals(100, count.get());
		
		count.set(0);
		status = supplier.receive(r->{
			if (r == null){
				return 60000L;
			}else{
				ids3.add(r.getSequenceNumber() + "-" + r.getSubSequenceNumber());
				if (count.incrementAndGet() < 210){
					return 60000L;
				}else{
					return -1L;
				}
			}
		}, status.getLastPosition(), Instant.now());
		assertEquals(210, count.get());
		
		assertEquals(ids.size(), ids3.size());
		Object[] ids3Array = ids3.stream().toArray();
		for (int i = 0; i < idsArray.length; i ++){
			if (!idsArray[i].equals(ids3Array[i])){
				System.out.println("Not the same at " + i + ":\t" + idsArray[i] + "\t" + ids3Array[i]);
			}
		}
		assertEquals(ids, ids3);
		
		// receive in one go
		Set<String> ids4 = new TreeSet<>();
		count.set(0);
		status = supplier.receive(r->{
			if (r == null){
				return 60000L;
			}else{
				ids4.add(r.getSequenceNumber() + "-" + r.getSubSequenceNumber());
				if (count.incrementAndGet() < 510){
					return 60000L;
				}else{
					return -1L;
				}
			}
		}, startPosition, Instant.now());
		assertEquals(510, count.get());

		assertEquals(ids.size(), ids4.size());
		Object[] ids4Array = ids4.stream().toArray();
		for (int i = 0; i < idsArray.length; i ++){
			if (!idsArray[i].equals(ids4Array[i])){
				System.out.println("Not the same at " + i + ":\t" + idsArray[i] + "\t" + ids4Array[i]);
			}
		}
		assertEquals(ids, ids4);
	}
	
	@Test
	public void test04ReceiveLatest() throws DataStreamInfrastructureException, InterruptedException {
		List<StreamDataSupplierWithId<UserRecord>> suppliersWithId = KinesisStreamDataSupplier.create(
				awsAccessKeyId, awsSecretKey, endpoint, streamName, x->x, 
				pollInterval, fetchBatchSize, receiveBatchSize);
		StreamDataSupplier<UserRecord> supplier = suppliersWithId.get(0).getSupplier();
		String lastPosition = ((KinesisStreamDataSupplier<UserRecord>)supplier).lastPosition(100);

		AtomicInteger count = new AtomicInteger();
		long finishTime = System.currentTimeMillis() + 60 * 1000L;
		ReceiveStatus status = supplier.receive(r->{
			if (r != null){
				System.out.println(r.getSequenceNumber() + "-" + r.getSubSequenceNumber() + " @ " + r.getApproximateArrivalTimestamp());
				count.incrementAndGet();
			}
			return finishTime - System.currentTimeMillis();
		}, lastPosition, (String)null);
		System.out.println(status);
		System.out.println(count);
		assertFalse(status.isOutOfRangeReached());
		
		count.set(0);
		status = supplier.receive(r->{
			if (r != null){
				System.out.println(r.getSequenceNumber() + "-" + r.getSubSequenceNumber() + " @ " + r.getApproximateArrivalTimestamp());
				count.incrementAndGet();
			}
			return 10000L;
		}, lastPosition, Instant.now().plusSeconds(10));
		System.out.println(status);
		System.out.println(count);
		assertTrue(status.isOutOfRangeReached());

	}

	
	protected Set<String> collectIds(Collection<UserRecord> records){
		return records.stream().map(r->r.getSequenceNumber() + "-" + r.getSubSequenceNumber()).collect(Collectors.toSet());
	}

}
