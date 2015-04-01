/*
Copyright 2015 Zhengmao HU (James Hu)

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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * A period of time qualified by a quantity and a unit.
 * @author James Hu
 *
 */
public class ChronoTimePeriod implements Comparable<ChronoTimePeriod>, Serializable{
	private static final long serialVersionUID = 3894027090839792451L;

	protected long amount;
	protected ChronoTimePeriodUnit unit;
	protected String shortString;			// as a cache
	
	public ChronoTimePeriod(ChronoTimePeriodUnit unit){
		this(1L, unit);
	}

	
	public ChronoTimePeriod(long quantity, ChronoTimePeriodUnit unit){
		this.amount = quantity;
		this.unit = unit;
	}
	
	public ChronoTimePeriod(ChronoTimePeriod other){
		this(other.amount, other.unit);
	}
	
	@Override
	public boolean equals(Object o){
		if (o == this){
			return true;
		}
		
		if (o != null && o instanceof ChronoTimePeriod){
			ChronoTimePeriod that = (ChronoTimePeriod)o;
			return new EqualsBuilder()
				.append(this.amount, that.amount)
				.append(this.unit, that.unit)
				.isEquals();
		}else{
			return false;
		}
	}
	
	@Override
	public int hashCode(){
		return (int) (amount * amount) + unit.hashCode() * 41;
	}
	
	@Override
	public int compareTo(ChronoTimePeriod that) {
		if (that == null){
			return 1;
		}
		long diff = this.toMilliseconds() - that.toMilliseconds();
		if (diff > 0){
			return 1;
		}else if (diff < 0){
			return -1;
		}else{
			return 0;
		}
	}

	
	/**
	 * Parse strings like '1 hour', '2 days', '3 Years', '12 minute' into TimePeriod.
	 * Short formats like '1H', '2 D', '3y' are also supported.
	 * @param quantityAndUnit	the string to be parsed
	 * @return	Both quantity and unit
	 */
	static public ChronoTimePeriod from(String quantityAndUnit) {
		String trimed = quantityAndUnit.trim();
		String allExceptLast = trimed.substring(0, trimed.length() - 1);
		if (StringUtils.isNumericSpace(allExceptLast)){ // short format
			long amount = Long.parseLong(allExceptLast.trim());
			ChronoTimePeriodUnit unit = ChronoTimePeriodUnit.from(Character.toUpperCase(trimed.charAt(trimed.length() - 1)));
			return new ChronoTimePeriod(amount, unit);
		}else{
			String[] durationAndUnit = StringUtils.split(trimed);
			Long amount = Long.valueOf(durationAndUnit[0]);
			ChronoTimePeriodUnit unit = ChronoTimePeriodUnit.from(durationAndUnit[1]);
			return new ChronoTimePeriod(amount, unit);
		}
	}

	static public ChronoTimePeriod of (TimePeriod timePeriod){
		return timePeriod == null ? null : new ChronoTimePeriod(timePeriod.getAmount(), ChronoTimePeriodUnit.of(timePeriod.getUnit()));
	}

	public TimePeriod toTimePeriod(){
		return new TimePeriod(amount, unit.toTimePeriodUnit());
	}
	
	public long toMilliseconds(){
		return amount * unit.toMilliseconds();
	}
	
	public boolean isDivisorOf(ChronoTimePeriod that){
		switch(that.unit){
			case YEARS:
				switch(this.unit){
					case YEARS:
					case WEEKS:
					case DAYS:
						return (that.amount % this.amount) == 0;
					case MONTHS:
						return ((that.amount * 12) % this.amount) == 0;
					default:
						return this.isDivisorOf(new ChronoTimePeriod(that.amount, ChronoTimePeriodUnit.DAYS));
				}
			case MONTHS:
				switch(this.unit){
					case YEARS:
						return (that.amount % (this.amount * 12)) == 0;
					case MONTHS:
					case WEEKS:
					case DAYS:
						return (that.amount % this.amount) == 0;
					default:
						return this.isDivisorOf(new ChronoTimePeriod(that.amount, ChronoTimePeriodUnit.DAYS));
				}
			case WEEKS:
				switch(this.unit){
					case YEARS:
					case MONTHS:
					case WEEKS:
					case DAYS:
						return (that.amount % this.amount) == 0;
					default:
						return this.isDivisorOf(new ChronoTimePeriod(that.amount, ChronoTimePeriodUnit.DAYS));
				}
			case DAYS:
				switch(this.unit){
					case WEEKS:
						return (that.amount % (this.amount * 7)) == 0;
					case YEARS:
					case MONTHS:
					case DAYS:
						return (that.amount % this.amount) == 0;
					default:
						return that.toMilliseconds() % this.toMilliseconds() == 0;
				}
			default:
				switch(this.unit){
					case YEARS:
					case MONTHS:
					case WEEKS:
						return (that.amount % this.amount) == 0;
					default:
						return that.toMilliseconds() % this.toMilliseconds() == 0;
				}
		}
	}
	
	@Override
	public String toString(){
		return String.valueOf(amount) + " " + unit.toShortCode();
	}
	
	public String toShortString(){
		if (shortString == null){
			shortString = String.valueOf(amount) + unit.toShortCode();
		}
		return shortString;
	}
	
	public long getAmount() {
		return amount;
	}
	public void setAmount(long amount) {
		this.amount = amount;
	}
	public ChronoTimePeriodUnit getUnit() {
		return unit;
	}
	public void setUnit(ChronoTimePeriodUnit unit) {
		this.unit = unit;
	}

}
