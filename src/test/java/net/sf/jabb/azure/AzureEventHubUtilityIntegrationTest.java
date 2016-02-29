/**
 * 
 */
package net.sf.jabb.azure;

import static org.junit.Assert.*;

import java.util.List;

import javax.jms.JMSException;

import net.sf.jabb.dstream.StreamDataSupplierWithId;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AzureEventHubUtilityIntegrationTest {

	@Test
	public void testCreateSuppliers() throws JMSException {
		List<StreamDataSupplierWithId<String>> suppliers = AzureEventHubUtility.createStreamDataSuppliers(
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"),
				AzureEventHubUtility.DEFAULT_CONSUMER_GROUP,
				msg -> "a");
		assertNotNull(suppliers);
		assertTrue(suppliers.size() >= 4);
	}

	@Test
	public void testGetPartitions() throws Exception{
		String[] partitions = AzureEventHubUtility.getPartitions(
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_HOST"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_NAME"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_RECEIVE_USER_PASSWORD"),
				System.getenv("SYSTEM_DEFAULT_AZURE_EVENT_HUB_NAME"));
		assertNotNull(partitions);
		assertTrue(partitions.length >= 4);
	}


}
