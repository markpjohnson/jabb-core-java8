package net.sf.jabb.seqtx.azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import net.sf.jabb.seqtx.SequentialTransactionsCoordinator;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinatorTest;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.microsoft.azure.storage.CloudStorageAccount;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureSequentialTransactionsCoordinatorIntegrationTest extends SequentialTransactionsCoordinatorTest{

	public AzureSequentialTransactionsCoordinatorIntegrationTest(){
		super();
		timeScale = 20;
	}
	
	static protected AzureSequentialTransactionsCoordinator createAzureCoordinator()  throws InvalidKeyException, URISyntaxException{
		String connectionString = System.getenv("SYSTEM_DEFAULT_AZURE_STORAGE_CONNECTION");
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
		AzureSequentialTransactionsCoordinator tracker = new AzureSequentialTransactionsCoordinator(storageAccount, "TestTable");
		return tracker;
		
	}
	
	@Override
	protected SequentialTransactionsCoordinator createCoordinator() throws InvalidKeyException, URISyntaxException {
		return createAzureCoordinator();
	}
	
	@BeforeClass
	@AfterClass
	static public void clearAll() throws TransactionStorageInfrastructureException, InvalidKeyException, URISyntaxException{
		createAzureCoordinator().clearAll();
	}
	

}
