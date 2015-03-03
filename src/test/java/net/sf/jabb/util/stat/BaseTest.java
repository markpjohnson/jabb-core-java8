/**
 * 
 */
package net.sf.jabb.util.stat;

import java.math.BigInteger;

import net.sf.jabb.util.stat.NumberGenerator;

/**
 * @author James Hu
 *
 */
public class BaseTest {
	protected int warmUpSeconds = 2;
	protected int testSeconds = 10;
	protected int testThreads = 50;
	protected int batchSize = 1000;
	protected int lengthOfRandomNumbers = batchSize * 1000;
	
	protected int[] randomIntegers = NumberGenerator.randomIntegers(Integer.MIN_VALUE, Integer.MAX_VALUE, lengthOfRandomNumbers);
	protected long[] randomLongs = NumberGenerator.randomLongs(Long.MIN_VALUE, Long.MAX_VALUE, lengthOfRandomNumbers);
	protected BigInteger[] randomBigIntegers = NumberGenerator.randomBigIntegers(Double.MIN_VALUE, Double.MAX_VALUE, lengthOfRandomNumbers);
	protected BigInteger[] randomIntegersAsBigIntegers = NumberGenerator.randomBigIntegers(Integer.MIN_VALUE, Integer.MAX_VALUE, lengthOfRandomNumbers);
	protected BigInteger[] randomLongsAsBigIntegers = NumberGenerator.randomBigIntegers(Long.MIN_VALUE, Long.MAX_VALUE, lengthOfRandomNumbers);

	protected BigInteger sumOfRandomIntegers = sum(randomIntegers);
	protected BigInteger sumOfRandomLongs = sum(randomLongs);
	protected BigInteger sumOfRandomBigIntegers = sum(randomBigIntegers);
	protected BigInteger sumOfRandomIntegersAsBigIntegers = sum(randomIntegersAsBigIntegers);
	protected BigInteger sumOfRandomLongsAsBigIntegers = sum(randomLongsAsBigIntegers);
	
	
	protected BigInteger sum(int[] values){
		BigInteger result = BigInteger.ZERO;
		for (int v: values){
			result = result.add(BigInteger.valueOf(v));
		}
		return result;
	}

	protected BigInteger sum(long[] values){
		BigInteger result = BigInteger.ZERO;
		for (long v: values){
			result = result.add(BigInteger.valueOf(v));
		}
		return result;
	}

	protected BigInteger sum(BigInteger[] values){
		BigInteger result = BigInteger.ZERO;
		for (BigInteger v: values){
			result = result.add(v);
		}
		return result;
	}
	
	protected BigInteger min(int[] values){
		BigInteger result = null;
		for (int v: values){
			if (result == null){
				result = BigInteger.valueOf(v);
			}else{
				result = result.min(BigInteger.valueOf(v));
			}
		}
		return result;
	}
	
	protected BigInteger min(long[] values){
		BigInteger result = null;
		for (long v: values){
			if (result == null){
				result = BigInteger.valueOf(v);
			}else{
				result = result.min(BigInteger.valueOf(v));
			}
		}
		return result;
	}
	
	protected BigInteger min(BigInteger[] values){
		BigInteger result = null;
		for (BigInteger v: values){
			if (result == null){
				result = v;
			}else{
				result = result.min(v);
			}
		}
		return result;
	}

	protected BigInteger max(int[] values){
		BigInteger result = null;
		for (int v: values){
			if (result == null){
				result = BigInteger.valueOf(v);
			}else{
				result = result.max(BigInteger.valueOf(v));
			}
		}
		return result;
	}
	
	protected BigInteger max(long[] values){
		BigInteger result = null;
		for (long v: values){
			if (result == null){
				result = BigInteger.valueOf(v);
			}else{
				result = result.max(BigInteger.valueOf(v));
			}
		}
		return result;
	}
	
	protected BigInteger max(BigInteger[] values){
		BigInteger result = null;
		for (BigInteger v: values){
			if (result == null){
				result = v;
			}else{
				result = result.max(v);
			}
		}
		return result;
	}

}
