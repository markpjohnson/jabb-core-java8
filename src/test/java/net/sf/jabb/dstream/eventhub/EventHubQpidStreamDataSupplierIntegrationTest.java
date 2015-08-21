/**
 * 
 */
package net.sf.jabb.dstream.eventhub;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.eventhub.EventHubQpidStreamDataSupplier;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class EventHubQpidStreamDataSupplierIntegrationTest {
	
	protected EventHubQpidStreamDataSupplier<String> create(){
		EventHubQpidStreamDataSupplier<String> sp = new EventHubQpidStreamDataSupplier<>(
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
				null, 0, 
				message -> {
					try{
						if (message instanceof TextMessage){
							return ((TextMessage)message).getText();
						}else if (message instanceof BytesMessage){
							BytesMessage msg = (BytesMessage) message;
							byte[] bytes = new byte[(int)msg.getBodyLength()];
							msg.readBytes(bytes, (int)msg.getBodyLength());
							return new String(bytes, StandardCharsets.UTF_8);
						}else{
							return message.getClass().getName();
						}
					}catch(Exception e){
						e.printStackTrace();
						return null;
					}
				});
		return sp;
	}

	@Test
	public void testGetByRange() throws Exception {
		StreamDataSupplier<String> sp = create();
		
		sp.start();
		try{
			List<String> result = new LinkedList<>();
			sp.fetch(result, sp.firstPosition(), "4302000000", Duration.ofMillis(1000));
					
			for (String s: result){
				System.out.println(s);
			}
		}finally{
			sp.stop();
		}
	}
	
	@Test
	public void testGetByMaxItems() throws Exception {
		StreamDataSupplier<String> sp = create();
		
		sp.start();
		try{
			List<String> result = new LinkedList<>();
			sp.fetch(result, sp.firstPosition(), 20, Duration.ofMillis(500));
					
			for (String s: result){
				System.out.println(s);
			}
		}finally{
			sp.stop();
		}
	}
	
	@Test
	public void testGetFirstPosition() throws Exception{
		EventHubQpidStreamDataSupplier<String> sp = create();
		sp.start();
		try{
			String firstPosition = sp.firstPosition();
			assertEquals(String.valueOf(-1), firstPosition);
			
			firstPosition = sp.firstPosition(Instant.EPOCH, Duration.ofSeconds(1));
			System.out.println(firstPosition);
			
			Message msg = sp.firstMessageByListener(sp.messageSelector(firstPosition), Duration.ofSeconds(5).toMillis());
			System.out.println(msg);

			firstPosition = sp.firstPosition(Instant.now().minusSeconds(3600L*24*3), Duration.ofSeconds(5));
			System.out.println(firstPosition);
			
			firstPosition = sp.firstPosition(Instant.now().minusSeconds(3600L*1), Duration.ofSeconds(5));
			System.out.println(firstPosition);
			
			assertNull(sp.firstPosition(Instant.now().plusSeconds(3600L*24*3), Duration.ofSeconds(1)));
			
		}finally{
			sp.stop();
		}
		
	}


	@Test
	public void testGetLastPosition() throws Exception{
		EventHubQpidStreamDataSupplier<String> sp = create();
		sp.start();
		try{
			String firstPosition = sp.firstPosition();
			String lastPosition = String.valueOf(sp.lastPositionByTryError());
			lastPosition = sp.lastPosition();
			
			System.out.println("first: " + firstPosition + ", last: " + lastPosition);
			
		}finally{
			sp.stop();
		}
	}
	
	@Test
	public void testStartAndStopReceiving() throws Exception{
		StreamDataSupplier<String> sp = create();
		sp.start();
		try{
			String id = sp.startAsyncReceiving(s -> System.out.println(s), sp.firstPosition());
			Thread.sleep(1000);
			sp.stopAsyncReceiving(id);
		}finally{
			sp.stop();
		}
		
	}


}
