/*
Copyright 2015 Zhengmao HU (James)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 提供基本的统计信息，包括：
 * 最大值、最小值、平均值、总计、个数。
 * 它是多线程安全的。
 * @author Zhengmao HU (James)
 */
public class ConcurrentLongStatistics implements NumberStatistics<Long>, Serializable {
	private static final long serialVersionUID = 2001318020408834046L;

	protected LongAdder count;
	protected LongAdder sum;
	protected ConcurrentLongMinMaxHolder minMax;
	
	public ConcurrentLongStatistics(){
		count = new LongAdder();
		sum = new LongAdder();
		minMax = new ConcurrentLongMinMaxHolder();
	}
	
	@Override
	public void evaluate(int value) {
		count.increment();
		sum.add(value);
		minMax.evaluate(value);
	}

	@Override
	public void evaluate(long value){
		count.increment();
		sum.add(value);
		minMax.evaluate(value);
	}
	
	@Override
	public void evaluate(BigInteger value) {
		long x = value.longValue();
		count.increment();
		sum.add(x);
		minMax.evaluate(x);
	}

	@Override
	public Double getAvg(){
		long countValue = count.sum();
		if (countValue > 0){
			return sum.doubleValue()/countValue;
		}else{
			return null;
		}
	}
	
	@Override
	public BigDecimal getAvg(int scale) {
		return new BigDecimal(getAvg()).setScale(scale, BigDecimal.ROUND_HALF_UP);
	}

	@Override
	public Long getMin() {
		return minMax.getMinAsLong();
	}
	
	@Override
	public Long getMax() {
		return minMax.getMaxAsLong();
	}
	
	@Override
	public Long getSum() {
		return sum.sum();
	}
	
	@Override
	public long getCount() {
		return count.sum();
	}
	
	@Override
	public void reset(){
		count.reset();
		sum.reset();
		minMax.reset();
	}

	@Override
	public void reset(long newCount, Long newSum, Long newMin, Long newMax) {
		count.reset();
		count.add(newCount);
		sum.reset();
		sum.add(newSum);
		minMax.reset(newMin, newMax);
	}

	@Override
	public String toString(){
		return "(" + count.sum() + ", " + sum.sum() + ", " + getMin() + "/" + getMax() + ")";
	}

	@Override
	public void merge(long count, Long sum, Long min, Long max) {
		this.count.add(count);
		if (sum != null){
			this.sum.add(sum);
		}
		if (min != null){
			this.minMax.evaluate(min);
		}
		if (max != null){
			this.minMax.evaluate(max);
		}
	}

	@Override
	public void merge(NumberStatistics<? extends Number> other){
		if (other != null && other.getCount() > 0){
			merge(other.getCount(), other.getSum().longValue(), other.getMin().longValue(), other.getMax().longValue());
		}
	}
	
}
