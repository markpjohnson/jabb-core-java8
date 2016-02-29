/**
 * 
 */
package net.sf.jabb.azure;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.StreamDataSupplierWithIdImpl;
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
	static private DateTimeFormatter iso8601Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).withZone(ZoneId.of("UTC"));
	
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
			if (response instanceof MapMessage){
				MapMessage map = (MapMessage) response;
				String[] partitions = (String[])map.getObject("partition_ids");
				return partitions;
			}else{
				throw new IllegalStateException("The Event Hub probably does not exist or is disabled: " + server + "/" + eventHubName 
						+ ". Response code: " + response.getObjectProperty("status-code") + ". Response description: " + response.getObjectProperty("status-description"));
			}
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
	 * Create a list of {@link StreamDataSupplierWithIdImpl}s from an Event Hub.
	 * @param <M>		type of the message
	 * @param server		the server name containing name space of the Event Hub
	 * @param policyName	policy with read permission
	 * @param policyKey		key of the policy
	 * @param eventHubName	name of the Event Hub
	 * @param consumerGroup		consumer group name
	 * @param messageConverter	JMS message converter
	 * @return					a list of {@link StreamDataSupplierWithIdImpl}s, one per partition
	 * @throws JMSException		If list of partitions cannot be fetched
	 */
	public static <M> List<StreamDataSupplierWithId<M>> createStreamDataSuppliers(String server, String policyName, String policyKey,
	                                                                              String eventHubName, String consumerGroup, Function<Message, M> messageConverter) throws JMSException{
		return EventHubQpidStreamDataSupplier.create(server, policyName, policyKey, eventHubName, consumerGroup, messageConverter);
	}
	
	public static String generateSharedAccessSignature(String stringToSign, byte[] keyBytes){
		SecretKey key256;
		Mac hmacSha256;
		key256 = new SecretKeySpec(keyBytes, "HmacSHA256");
        try {
            hmacSha256 = Mac.getInstance("HmacSHA256");
            hmacSha256.init(key256);
        }catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("No HmacSHA256 support in JVM", e);
        }catch (InvalidKeyException e){
            throw new IllegalArgumentException("The key is not suitable for signing", e);
        }
        
        byte[] utf8Bytes = null;
        utf8Bytes = stringToSign.getBytes(StandardCharsets.UTF_8);

        String signature = Base64.getEncoder().encodeToString(hmacSha256.doFinal(utf8Bytes));
 
        return signature;
	}
	
	/**
	 * Generate the SAS token for accessing an Event Hub
	 * @param serviceNameSpace		name space of the service bus service
	 * @param servicePath			name of the Event Hub, or a path specifying other additional information
	 * @param policyName				access policy name
	 * @param policyKey				access policy key
	 * @param expiration			the time that the generated token will expiere
	 * @return	the full token: {@code SharedAccessSignature sr={URI}&sig={HMAC_SHA256_SIGNATURE}&se={EXPIRATION_TIME}&skn={KEY_NAME} }
	 */
	public static String generateSharedAccessSignatureToken(String serviceNameSpace, String servicePath, String policyName, String policyKey, Instant expiration){
		long expireEpochSeconds = expiration.getEpochSecond();
		String escapedUri = getEscapedUri(serviceNameSpace, servicePath);
		
		//byte[] policyKeyBytes = Base64.getDecoder().decode(policyKey);
		byte[] policyKeyBytes = policyKey.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        
        sb.append(escapedUri).append('\n'); // resource name
        sb.append(expireEpochSeconds);
        
        String stringToSign;
        stringToSign = sb.toString();
        
        String signature = generateSharedAccessSignature(stringToSign, policyKeyBytes);
        
        String escapedSignature;
        try {
			escapedSignature = URLEncoder.encode(signature, "UTF-8");
		} catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("No UTF8 support in JVM", e);
		}
		
		return "SharedAccessSignature sr=" + escapedUri
				+ "&sig=" + escapedSignature + "&se=" + expireEpochSeconds + "&skn=" + policyName;
	}
	
	private static String getUri(String serviceNameSpace, String servicePath){
		String plain = "https://" + serviceNameSpace + ".servicebus.windows.net/" + servicePath;
		return plain;
	}
	

	private static String getEscapedUri(String serviceNameSpace, String servicePath){
		String plain = getUri(serviceNameSpace, servicePath);
		try {
			return URLEncoder.encode(plain, "UTF-8").toLowerCase();
		} catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("No UTF8 support in JVM", e);
		}
	}
	
}
