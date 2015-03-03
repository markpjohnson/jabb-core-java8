/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AddersTest extends BaseTest{
	protected int batchs = batchSize/10;
	
	@Test
	public void testAssumptions() {
		assertEquals(Long.MAX_VALUE, 2L * (2L + Integer.MAX_VALUE) * Integer.MAX_VALUE + 1);
	}

	@Test
	public void testLongAdder() throws InterruptedException{
		LongAdder longAdder = new LongAdder();
		doTestAdder(longAdder);
	}
	
	@Test
	public void testBigIntegerAdder() throws InterruptedException{
		BigIntegerAdder bigIntegerAdder = new BigIntegerAdder();
		doTestAdder(bigIntegerAdder);
	}

	protected void doTestAdder(LongAdder adder) throws InterruptedException {
		adder.reset();
		Thread t1 = new Thread(()->{
			for (int b = 0; b < batchs; b ++){
				for (int v: randomIntegers){
					adder.add(v);
				}
			}
		});
		Thread t2 = new Thread(()->{
			for (int b = 0; b < batchs; b ++){
				for (long v: randomLongs){
					adder.add(v);
				}
			}
		});
		
		t1.start();
		t2.start();
		t1.join();
		t2.join();
		
		BigInteger expectedResult = sumOfRandomIntegers.add(sumOfRandomLongs).multiply(BigInteger.valueOf(batchs));
		assertEquals(expectedResult.longValue(), adder.sum());
	}
	
	protected void doTestAdder(BigIntegerAdder adder) throws InterruptedException{
		adder.reset();
		Thread t1 = new Thread(()->{
			for (int b = 0; b < batchs; b ++){
				for (int v: randomIntegers){
					adder.add(v);
				}
			}
		});
		Thread t2 = new Thread(()->{
			for (int b = 0; b < batchs; b ++){
				for (long v: randomLongs){
					adder.add(v);
				}
			}
		});
		Thread t3 = new Thread(()->{
			for (int b = 0; b < batchs; b ++){
				for (BigInteger v: randomBigIntegers){
					adder.add(v);
				}
			}
		});
		
		t1.start();
		t2.start();
		t3.start();
		t1.join();
		t2.join();
		t3.join();
		
		BigInteger expectedResult = sumOfRandomIntegers.add(sumOfRandomLongs).add(sumOfRandomBigIntegers).multiply(BigInteger.valueOf(batchs));
		assertEquals(expectedResult, adder.sum());
	}
	

}
