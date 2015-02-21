/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;

/**
 * Multi-thread safe statistics holder for high precision use cases.
 * @author James Hu
 *
 */
public class AdvancedNumberStatistics {
	
	BigIntegerAdder count;
	BigIntegerAdder sum;
	AtomicMinMaxBigInteger minMax;
	
	public void put(BigInteger x){
		count.add(1);
		sum.add(x);
		minMax.minMax(x);
	}
	
	public void put(int x){
		minMax.minMax(BigInteger.valueOf(x));
		sum.add(x);
		count.add(1);
		
	}
	
	public void put(long x){
		BigInteger bx = BigInteger.valueOf(x);
		minMax.minMax(bx);
		if (x <= Integer.MAX_VALUE){
			sum.add((int)x);
		}else{
			sum.add(bx);
		}
		count.add(1);
	}
}
