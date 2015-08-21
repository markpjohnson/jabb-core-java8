/**
 * 
 */
package net.sf.jabb.azure;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.eventhub.EventHubQpidStreamDataSupplier;
import net.sf.jabb.util.jms.JmsUtility;

import org.apache.qpid.amqp_1_0.jms.impl.MessageImpl;

import com.google.common.base.Throwables;

/**
 * Utility methods for Azure Event Hub.
 * @author James Hu
 *
 */
public class AzureEventHubUtility {
	static public final String DEFAULT_CONSUMER_GROUP = "$Default";
	
	public static EventHubAnnotations getEventHubAnnotations(Message message){
		if (message == null){
			return null;
		}
		
		String json = null;
		try {
			json = message.getStringProperty(MessageImpl.JMS_AMQP_MESSAGE_ANNOTATIONS);
		} catch (JMSException e) {
			throw Throwables.propagate(e);
		}
		EventHubAnnotations annotations = new EventHubAnnotations(json);
		return annotations;
	}
	
	static public String[] getPartitions(String server, String policyName, String policyKey, 
			String eventHubName) throws JMSException {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		MessageProducer sender = null;
		
		String queueName = "$management";
		org.apache.qpid.amqp_1_0.jms.impl.QueueImpl mgmtQueue = new org.apache.qpid.amqp_1_0.jms.impl.QueueImpl(queueName);

		try{
			ConnectionFactory connectionFactory = createConnectionFactory(server, policyName, policyKey, "");
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			sender = session.createProducer(mgmtQueue);
			consumer = session.createConsumer(mgmtQueue);
			connection.start();
			Message request = session.createStreamMessage();
			request.setStringProperty("operation", "READ");
			request.setStringProperty("type", "com.microsoft:eventhub");
			request.setStringProperty("name", eventHubName);
			sender.send(request);
			Message response = consumer.receive();
			connection.stop();
			MapMessage map = (MapMessage) response;
			String[] partitions = (String[])map.getObject("partition_ids");
			return partitions;
		}finally{
			JmsUtility.closeSilently(sender, consumer, session);
			JmsUtility.closeSilently(connection);
		}
	}

	static public ConnectionFactory createConnectionFactory(String server, String policyName, String policyKey, String clientId){
		org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl connectionFactory = new org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl(
				"amqps", server, 5671, policyName, policyKey, clientId, 
				server, true, 0);
		connectionFactory.setSyncPublish(false);
		return connectionFactory;
	}
	

	
	/**
	 * Create a list of {@link StreamDataSupplierWithId}s from an Event Hub.
	 * @param <M>		type of the message
	 * @param server		the server name containing name space of the Event Hub
	 * @param policyName	policy with read permission
	 * @param policyKey		key of the policy
	 * @param eventHubName	name of the Event Hub
	 * @param consumerGroup		consumer group name
	 * @param messageConverter	JMS message converter
	 * @return					a list of {@link StreamDataSupplierWithId}s, one per partition
	 * @throws JMSException		If list of partitions cannot be fetched
	 */
	public static <M> List<StreamDataSupplierWithId<M>> createStreamDataSuppliers(String server, String policyName, String policyKey, 
			String eventHubName, String consumerGroup, Function<Message, M> messageConverter) throws JMSException{
		String[] partitions = getPartitions(server, policyName, policyKey, eventHubName);
		List<StreamDataSupplierWithId<M>> suppliers = new ArrayList<>(partitions.length);
		for (String partition: partitions){
			EventHubQpidStreamDataSupplier<M> supplier = new EventHubQpidStreamDataSupplier<>(server, eventHubName, policyName, policyKey,
					consumerGroup, partition, messageConverter);
			suppliers.add(new StreamDataSupplierWithId<>(partition, supplier));
		}
		return suppliers;
	}
	
}
