/**
 * 
 */
package net.sf.jabb.dstream.mock;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class MockedStreamDataSupplierTest {

	@Test
	public void testFirstPosition() throws DataStreamInfrastructureException, InterruptedException {
		for (int i = 0; i < 100; i ++){
			doTestFirstPosition(1);
			doTestFirstPosition(2);
			doTestFirstPosition(10);
			doTestFirstPosition(50);
			doTestFirstPosition(100);
			doTestFirstPosition(200);
			doTestFirstPosition(500);
			doTestFirstPosition(1000);
		}
	}
	
	protected void doTestFirstPosition(int eventsPerSecond) throws DataStreamInfrastructureException, InterruptedException{
		int eventsIntervalMillis = 1000/eventsPerSecond;
		Instant streamStartTime = Instant.now().minus(Duration.ofMinutes(1));
		StreamDataSupplier<String> sds = new MockedStreamDataSupplier(eventsPerSecond, streamStartTime, null);
		assertEquals("-1", sds.firstPosition());
		assertEquals(String.valueOf((streamStartTime.toEpochMilli()/eventsIntervalMillis+1)*eventsIntervalMillis), sds.firstPosition(Instant.now().minus(Duration.ofMinutes(2))));
		
		long t = System.currentTimeMillis();
		long et = (t / eventsIntervalMillis + 1) * eventsIntervalMillis;
		assertEquals(String.valueOf(et), sds.firstPosition(Instant.ofEpochMilli(t)));
	}

	@Test
	public void testLastPosition() throws DataStreamInfrastructureException{
		for (int i = 0; i < 100; i ++){
			doTestLastPosition(1);
			doTestLastPosition(2);
			doTestLastPosition(10);
			doTestLastPosition(50);
			doTestLastPosition(100);
			doTestLastPosition(200);
			doTestLastPosition(500);
			doTestLastPosition(1000);
		}
	}
	
	protected void doTestLastPosition(int eventsPerSecond) throws DataStreamInfrastructureException{
		int eventsIntervalMillis = 1000/eventsPerSecond;
		Instant streamStartTime = Instant.now().minus(Duration.ofMinutes(3));
		Instant streamEndTime = Instant.now().minus(Duration.ofMinutes(1));
		StreamDataSupplier<String> sds = new MockedStreamDataSupplier(eventsPerSecond, streamStartTime, streamEndTime);
		assertEquals(String.valueOf(streamEndTime.toEpochMilli()/eventsIntervalMillis*eventsIntervalMillis), sds.lastPosition());
		
		sds = new MockedStreamDataSupplier(eventsPerSecond, streamStartTime, null);
		long et1 = (System.currentTimeMillis() / eventsIntervalMillis - 1) * eventsIntervalMillis;
		long lt = Long.parseLong(sds.lastPosition());
		long et2 = (System.currentTimeMillis() / eventsIntervalMillis + 1) * eventsIntervalMillis;
		assertTrue("last postion (" + lt + ") should be greater than " + et1, lt > et1);
		assertTrue("last postion (" + lt + ") should be no more than " + et2, lt <= et2);
		
	}
	
	@Test
	public void testFetch() throws DataStreamInfrastructureException, InterruptedException{
		for (int i = 0; i < 10; i ++){
			doTestFetch(1);
			doTestFetch(2);
			doTestFetch(10);
			doTestFetch(50);
			doTestFetch(100);
			doTestFetch(200);
			doTestFetch(500);
			doTestFetch(1000);
		}
	}

	protected void doTestFetch(int eventsPerSecond) throws DataStreamInfrastructureException, InterruptedException{
		int eventsIntervalMillis = 1000/eventsPerSecond;
		Instant streamStartTime = Instant.now().minus(Duration.ofMinutes(3));
		Instant streamEndTime = Instant.now().minus(Duration.ofMinutes(1));
		StreamDataSupplier<String> sds = new MockedStreamDataSupplier(eventsPerSecond, streamStartTime, streamEndTime);
		
		List<String> list = new ArrayList<String>();
		ReceiveStatus receiveStatus = sds.fetch(list, Instant.EPOCH, streamStartTime.plus(Duration.ofMinutes(1)), Duration.ofMillis(1000));
		assertEquals(60*eventsPerSecond, list.size());

		list.clear();
		receiveStatus = sds.fetch(list, streamStartTime.plus(Duration.ofSeconds(10)), streamStartTime.plus(Duration.ofSeconds(40)), Duration.ofMillis(1000));
		assertEquals(30*eventsPerSecond, list.size());
	}
}
