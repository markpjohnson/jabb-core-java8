/**
 * 
 */
package net.sf.jabb.dstream.eventhub;

import static org.junit.Assert.*;
import net.sf.jabb.azure.EventHubAnnotations;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class EventHubQpidDataStreamProviderTest {
	private static final String ANNOTATION_STRING = "{ \"x-opt-sequence-number\" : { \"long\" : 19650 }, \"x-opt-offset\" : \"4302249320\", \"x-opt-enqueued-time\" : { \"timestamp\" : 1438740137186 } }";

	@Test
	public void testParsingAnnotationString() {
		String json = ANNOTATION_STRING;
		
		EventHubAnnotations a = new EventHubAnnotations(json);
		assertNotNull(a);
		assertEquals(4302249320L, a.getOffset());
		assertEquals(19650L, a.getSequenceNumber());
		assertEquals(1438740137186L, a.getEnqueuedTime().toEpochMilli());
	}

}
