/**
 * 
 */
package net.sf.jabb.azure;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Date;
import java.util.Locale;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.microsoft.azure.storage.RequestOptions;
import com.microsoft.azure.storage.ServiceClient;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.core.SharedAccessSignatureHelper;
import com.microsoft.azure.storage.queue.SharedAccessQueuePolicy;

/**
 * @author James Hu
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureEventHubUtilityTest {
	String serviceNameSpace = "test-service";
	String servicePath = "eventhubname";
	String policyName = "SendRule";
	String policyKey = "mPCCyS2sxCj+l1RdA2MbNWjqZabWHF4DatX4Ch6gcCU=";
	Instant expiration = Instant.ofEpochSecond(1538265600);
	String correctResult = "SharedAccessSignature sr=https%3a%2f%2ftest-service.servicebus.windows.net%2feventhubname&sig=BNyISt7v4EoBgeeqn5kPgGZGJ0Vg0M%2fK3tY%2bi40oWPo%3d&se=1538265600&skn=SendRule";

	@Test
	public void test2GenerateSharedAccessSignatureToken() throws UnsupportedEncodingException {
		System.out.println(correctResult);
		String result = AzureEventHubUtility.generateSharedAccessSignatureToken(serviceNameSpace, servicePath, policyName, policyKey, expiration);
		System.out.println(result);
		assertEquals(URLDecoder.decode(correctResult, "UTF-8"), URLDecoder.decode(result, "UTF-8"));
	}

	
	@Test
	public void test1Signing() throws InvalidKeyException, StorageException, URISyntaxException, UnsupportedEncodingException{
		String uriString = "https://" + serviceNameSpace + ".servicebus.windows.net/" + servicePath;
		StorageCredentialsAccountAndKey credentials = new StorageCredentialsAccountAndKey(serviceNameSpace, policyKey);
		StorageUri uri = new StorageUri(new URI(uriString));
		ServiceClient client = new ServiceClient(uri, credentials){

			@Override
			public RequestOptions getDefaultRequestOptions() {
				// TODO Auto-generated method stub
				return null;
			}
			
		};
		
		SharedAccessQueuePolicy policy = new SharedAccessQueuePolicy();
		policy.setSharedAccessExpiryTime(Date.from(expiration));
		String correctResult = SharedAccessSignatureHelper.generateSharedAccessSignatureHashForQueue(
				policy, 
				policyName, 
				URLEncoder.encode(uriString, "UTF-8").toLowerCase(), 
				client,
				null);
		
		DateTimeFormatter iso8601Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).withZone(ZoneId.of("UTC"));
		String stringToSign = "\n\n"
				+ iso8601Formatter.format(expiration) + "\n"
				+ uriString + "\n"
				+ policyName + "\n"
				+ "2014-02-14";
		
		byte[] keyBytes = Base64.getDecoder().decode(policyKey);
		String result = AzureEventHubUtility.generateSharedAccessSignature(stringToSign, keyBytes);
		
		assertEquals(correctResult, result);
		System.out.println(result);
	}
}
