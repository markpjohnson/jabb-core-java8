package net.sf.jabb.seqtx.azure;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.LinkedList;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.microsoft.azure.storage.CloudStorageAccount;

import net.sf.jabb.seqtx.SequentialTransactionsCoordinator.TransactionCounts;
import net.sf.jabb.seqtx.SimpleSequentialTransaction;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.ex.InfrastructureErrorException;
import net.sf.jabb.seqtx.mem.InMemSequentialTransactionsCoordinator;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureSequentialTransactionsCoordinatorTest extends SequentialTransactionsCoordinatorTest{

	public AzureSequentialTransactionsCoordinatorTest(){
		super();
		timeScale = 20;
	}
	
	
	@Override
	protected SequentialTransactionsCoordinator createCoordinator() throws InvalidKeyException, URISyntaxException {
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		SequentialTransactionsCoordinator tracker = new AzureSequentialTransactionsCoordinator(storageAccount, "TestTable");
		return tracker;
	}
	
	@Test
	public void test00ClearAll() throws InfrastructureErrorException{
		tracker.clearAll();
	}
}
