/**
 * 
 */
package net.sf.jabb.azure;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AzureStorageUtilityTest {

	@Test
	public void testRetryIntervalCalculation() {
		long sum = 0;
		for (int i = 1; i <= 100; i ++){
			long interval = AzureStorageUtility.calculateRetryInterval(i, 5);
			sum += interval;
			System.out.print(i);
			System.out.print('\t');
			System.out.print(interval);
			System.out.println("\t" + sum);
			
		}
	}

}
