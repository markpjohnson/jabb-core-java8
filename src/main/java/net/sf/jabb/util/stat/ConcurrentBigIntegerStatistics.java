/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe statistics holder for high precision use cases.
 * @author James Hu
 *
 */
public class ConcurrentBigIntegerStatistics implements NumberStatistics<BigInteger>, Serializable{
	private static final long serialVersionUID = 5258045107455137348L;
	protected static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
	protected static final BigInteger LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
	
	protected LongAdder count;
	protected BigIntegerAdder sum;
	protected ConcurrentBigIntegerMinMaxHolder bigIntegerMinMax;
	protected ConcurrentLongMinMaxHolder longMinMax;
	
	public ConcurrentBigIntegerStatistics(){
		count = new LongAdder();
		sum = new BigIntegerAdder();
		bigIntegerMinMax = new ConcurrentBigIntegerMinMaxHolder();
		longMinMax = new ConcurrentLongMinMaxHolder();
	}
	
	public ConcurrentBigIntegerStatistics(int sumConcurrencyFactor){
		count = new LongAdder();
		sum = new BigIntegerAdder(sumConcurrencyFactor);
		bigIntegerMinMax = new ConcurrentBigIntegerMinMaxHolder();
		longMinMax = new ConcurrentLongMinMaxHolder();
	}
	
	@Override
	public void evaluate(BigInteger x){
		count.increment();
		sum.add(x);
		bigIntegerMinMax.evaluate(x);
	}
	
	@Override
	public void evaluate(int x){
		count.increment();
		sum.add(x);
		longMinMax.evaluate(x);
	}
	
	@Override
	public void evaluate(long x){
		count.increment();
		sum.add(x);
		longMinMax.evaluate(x);
	}
	
	@Override
	public long getCount(){
		return count.sum();
	}
	
	@Override
	public BigInteger getSum(){
		return sum.sum();
	}
	
	@Override
	public BigInteger getMin(){
		BigInteger bigIntegerMin = bigIntegerMinMax.getMin();
		Long longMin = longMinMax.getMinAsLong();
		if (bigIntegerMin == null){
			return longMin == null ? null : BigInteger.valueOf(longMin);
		}else{
			return longMin == null ? bigIntegerMin : bigIntegerMin.max(BigInteger.valueOf(longMin));
		}
	}
	
	@Override
	public BigInteger getMax(){
		BigInteger bigIntegerMax = bigIntegerMinMax.getMax();
		Long longMax = longMinMax.getMaxAsLong();
		if (bigIntegerMax == null){
			return longMax == null ? null : BigInteger.valueOf(longMax);
		}else{
			return longMax == null ? bigIntegerMax : bigIntegerMax.max(BigInteger.valueOf(longMax));
		}
	}
	
	@Override
	public void reset(){
		count.reset();
		sum.reset();
		bigIntegerMinMax.reset();
		longMinMax.reset();
	}
	
	@Override
	public void reset(BigInteger newCount, BigInteger newSum, BigInteger newMin, BigInteger newMax){
		count.reset();
		count.add(newCount.longValue());
		sum.set(newSum);
		if (newMax.compareTo(LONG_MAX_VALUE) <= 0 && newMin.compareTo(LONG_MIN_VALUE) >= 0){
			longMinMax.reset(newMin.longValue(), newMax.longValue());
			bigIntegerMinMax.reset();
		}else{
			bigIntegerMinMax.reset(newMin, newMax);
			longMinMax.reset();
		}
	}
	
	public void merge(ConcurrentBigIntegerStatistics another){
		count.add(another.count.sum());
		sum.add(another.getSum());
		bigIntegerMinMax.merge(another.bigIntegerMinMax);
		longMinMax.merge(another.longMinMax);
	}

	@Override
	public void merge(NumberStatistics<? extends Number> another) {
		if (another instanceof ConcurrentBigIntegerStatistics){
			merge((ConcurrentBigIntegerStatistics)another);
		}else{
			Number anotherCount = another.getCount();
			if (anotherCount != null && anotherCount.intValue() > 0){
				count.add(another.getCount());
				sum.add(another.getSum().longValue());
				longMinMax.evaluate(another.getMin().longValue());
				longMinMax.evaluate(another.getMax().longValue());
			}
		}
		
	}

	@Override
	public Double getAvg() {
		BigDecimal avg = getAvg(30);
		return avg == null? null : avg.doubleValue();
	}

	@Override
	public BigDecimal getAvg(int scale) {
		long countValue = count.sum();
		if (countValue > 0){
			BigDecimal avg = new BigDecimal(sum.sum(), scale);
			return avg.divide(new BigDecimal(countValue), BigDecimal.ROUND_HALF_UP);
		}else{
			return null;
		}
	}

}
