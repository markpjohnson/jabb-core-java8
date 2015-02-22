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
	
	BigIntegerAdder count = new BigIntegerAdder();;
	BigIntegerAdder sum = new BigIntegerAdder();
	AtomicMinMaxBigInteger minMax = new AtomicMinMaxBigInteger();
	
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
	
	public BigInteger getCount(){
		return count.sum();
	}
	
	public BigInteger getSum(){
		return sum.sum();
	}
	
	public BigInteger getMin(){
		return minMax.getMin();
	}
	
	public BigInteger getMax(){
		return minMax.getMax();
	}
	
	public void reset(){
		count.reset();
		sum.reset();
		minMax.reset();
	}
	
	public void merge(AdvancedNumberStatistics another){
		count.set(getCount().add(another.getCount()));
		sum.set(getSum().add(another.getSum()));
		minMax.merge(another.minMax);
	}
	
}
