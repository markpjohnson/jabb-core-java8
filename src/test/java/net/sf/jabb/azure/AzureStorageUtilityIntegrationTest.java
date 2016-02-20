/**
 * 
 */
package net.sf.jabb.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.attempt.TooManyAttemptsException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

import com.google.common.collect.Iterators;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;

/**
 * @author James Hu
 *
 */
public class AzureStorageUtilityIntegrationTest {
	static final String TEST_TABLE_NAME = "TestTable";
	
	protected CloudStorageAccount createStorageAccount() throws InvalidKeyException, URISyntaxException{
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		return storageAccount;
	}
	
	@Test
	public void testGeneratedFilters() throws URISyntaxException, StorageException, InvalidKeyException {
		CloudStorageAccount storageAccount = createStorageAccount();
		CloudTableClient tableClient = storageAccount.createCloudTableClient();
		AzureStorageUtility.deleteIfExists(tableClient, TEST_TABLE_NAME);
		AzureStorageUtility.createIfNotExists(tableClient, TEST_TABLE_NAME);
		
		CloudTable table = tableClient.getTableReference(TEST_TABLE_NAME);
		try{
			doTestGeneratedFilters(tableClient, table);
		}finally{
			AzureStorageUtility.deleteIfExists(tableClient, TEST_TABLE_NAME);
		}
	}
	
	protected void doTestGeneratedFilters(CloudTableClient tableClient, CloudTable table) throws StorageException{
		DynamicTableEntity entity = new DynamicTableEntity();
		
		entity.setPartitionKey("pk");
		for (String k: new String[]{
			"abcdefg",	
			"abcdefg ax",	
			"abcdefg|",	
			"abcdefg|x",	
			"abcdefg|中文",	
			"abcdefg2",	
			"abcdefg人",	
			"abcdefg￮",		// FFEE
			"abcdefg\uFFFE",
			"abcdefg\uFFFF",
			"abcdefg" + new String(Character.toChars(0x1D350)),	
			"abcdefg" + new String(Character.toChars(0x1F6F0)),	
			"abcdefh\uFFFE",
			"abcdefh\uFFFF",
			"abcdefh" + new String(Character.toChars(0x1D350)),	
			"abcdefh" + new String(Character.toChars(0x1F6F0)),	
			"中文开头",
			"中文开头 ",
			"中文开头| ",
			"中文开头| ￮",
			"中文开头|汉字 ￮",
			"中文开头|汉字好 ￮",
			"中文开头￮ ￮",
			"中文开头\uFFFE",
			"中文开头\uFFFF",
			"中文开头" + new String(Character.toChars(0x1D350)),	
			"中文开头" + new String(Character.toChars(0x1F6F0)),	
		}){
			entity.setRowKey(k);
			entity.getProperties().put("prop", new EntityProperty(k));
			table.execute(TableOperation.insert(entity));
			//System.out.println("Created: " + k);
		}
		
		testGenerateStartWithFilter(table, "a", 16);
		testGenerateStartWithFilter(table, "abcdefg", 12);
		testGenerateStartWithFilter(table, "abcdefg|", 3);
		testGenerateStartWithFilter(table, "abcdefh", 4);
		testGenerateStartWithFilter(table, "中文", 11);
		testGenerateStartWithFilter(table, "中文开头|", 4);
		testGenerateStartWithFilter(table, "abcdefg人", 1);
		testGenerateStartWithFilter(table, "abcdefg\uFFFE", 1);
		testGenerateStartWithFilter(table, "abcdefg\uFFFF", 1);
		testGenerateStartWithFilter(table, "abcdefg" + new String(Character.toChars(0x1D350)), 1);
		testGenerateStartWithFilter(table, "中文开头|汉字", 2);
		testGenerateStartWithFilter(table, "中文开头" + new String(Character.toChars(0x1F6F0)), 1);
		
		testGenerateStartToEndFilter(table, "a", "中文", 16);
		testGenerateStartToEndFilter(table, "abcdefg", "abcdefh", 12);
		testGenerateStartToEndFilter(table, "abcdefg1", "abcdefg9", 1);
		testGenerateStartToEndFilter(table, "中文", "中文开头|汉字", 4);
		
		// bugs in azure?
//		testGenerateStartToEndFilter(table, "中文", "中文开头￮ ", 6);
//		testGenerateStartToEndFilter(table, "中文", "中文开头" + new String(Character.toChars(0x1F6F0)), 10);
//		testGenerateStartToEndFilter(table, "中文开头\uFFFE", "中文开头" + new String(Character.toChars(0x1F6F0)), 3);
		
	}
	
	protected void testGenerateStartWithFilter(CloudTable table, String prefix, int count){
		testGenerateStartWithFilter(table, AzureStorageUtility.ROW_KEY, prefix, count);
		testGenerateStartWithFilter(table, "prop", prefix, count);
	}
	protected void testGenerateStartWithFilter(CloudTable table, String prop, String prefix, int count){
		String filter = AzureStorageUtility.generateStartWithFilterCondition(prop, prefix);
		TableQuery<DynamicTableEntity> query = TableQuery.from(DynamicTableEntity.class).where(filter);
		assertEquals(filter, count, Iterators.size(table.execute(query).iterator()));
	}
	
	protected void testGenerateStartToEndFilter(CloudTable table, String start, String end, int count){
		testGenerateStartToEndFilter(table, AzureStorageUtility.ROW_KEY, start, end, count);
		testGenerateStartToEndFilter(table, "prop", start, end, count);
	}
	protected void testGenerateStartToEndFilter(CloudTable table, String prop, String start, String end, int count){
		String filter = AzureStorageUtility.generateStartToEndFilterCondition(prop, start, end);
		TableQuery<DynamicTableEntity> query = TableQuery.from(DynamicTableEntity.class).where(filter);
		assertEquals(filter, count, Iterators.size(table.execute(query).iterator()));
	}
	
}
