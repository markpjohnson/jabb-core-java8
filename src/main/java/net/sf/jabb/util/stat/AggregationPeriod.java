/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * A period of time qualified by a quantity and a unit.
 * @author James Hu
 *
 */
public class AggregationPeriod implements Serializable, Comparable<AggregationPeriod>{
	private static final long serialVersionUID = -492234416175692243L;
	
	protected int amount;
	protected AggregationPeriodUnit unit;
	protected String codeName;			// as a cache
	
	/**
	 * Constructor
	 * @param amount	the amount
	 * @param unit		the unit
	 */
	public AggregationPeriod(int amount, AggregationPeriodUnit unit){
		this.amount = amount;
		this.unit = unit;
	}
	
	/**
	 * Return a copy of this with a new amount
	 * @param newAmount	the new amount
	 * @return	a new instance with the new amount and the same unit as this
	 */
	public AggregationPeriod withAmount(int newAmount){
		return new AggregationPeriod(newAmount, unit);
	}
	
	/**
	 * Return a copy of this
	 * @return	the new instance with the same amount and unit
	 */
	public AggregationPeriod copy(){
		return new AggregationPeriod(amount, unit);
	}
	
	/**
	 * Parse strings like '1 hour', '2 days', '3 Years', '12 minute' into AggregationPeriod.
	 * Short formats like '1H', '2 D', '3y' are also supported.
	 * @param amountAndUnit	the string to be parsed
	 * @return	the AggregationPeriod instance with specified amount and unit
	 */
	public static AggregationPeriod parse(String amountAndUnit){
		String trimed = amountAndUnit.trim();
		String allExceptLast = trimed.substring(0, trimed.length() - 1);
		if (StringUtils.isNumericSpace(allExceptLast)){ // short format
			int amount = Integer.parseInt(allExceptLast);
			AggregationPeriodUnit unit = AggregationPeriodUnit.valueOf(Character.toUpperCase(trimed.charAt(trimed.length() - 1)));
			return new AggregationPeriod(amount, unit);
		}else{
			String[] durationAndUnit = StringUtils.split(trimed);
			int amount = Integer.valueOf(durationAndUnit[0]);
			
			String unitString = durationAndUnit[1].toUpperCase();
			if(unitString.charAt(unitString.length() - 1) != 'S') {
				unitString = unitString + "S";
			}
			AggregationPeriodUnit unit = AggregationPeriodUnit.valueOf(unitString);

			return new AggregationPeriod(amount, unit);
		}
	}

	@Override
	public boolean equals(Object o){
		if (o == this){
			return true;
		}
		
		if (o != null && o instanceof AggregationPeriod){
			AggregationPeriod that = (AggregationPeriod)o;
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
		return amount * 799991 + amount + unit.hashCode();
	}

	/**
	 * Determine if the statistics for this period can be used to generate the statistics of an upper level period.
	 * @param upperLevel	the upper level period
	 * @return	true if aggregation is supported, false if not.
	 */
	public boolean canBeAggregatedTo(AggregationPeriod upperLevel){
		return unit.canSupportAggregation(amount, upperLevel.unit, upperLevel.amount);
	}
	
	@Override
	public String toString(){
		return String.valueOf(amount) + " " + unit.toString();
	}
	
	public String getCodeName(){
		if (codeName == null){
			codeName = getCodeName(amount, unit);
		}
		return codeName;
	}
	
	static public String getCodeName(int amount, AggregationPeriodUnit unit){
		return String.valueOf(amount) + unit.getCode();
	}
	
	/**
	 * Get the duration that may be estimated
	 * @return	the duration of this aggregation period
	 */
	public Duration getDuration(){
		return unit.getTemporalUnit().getDuration().multipliedBy(amount);
	}
	
	@Override
	public int compareTo(AggregationPeriod that) {
		if (that == null){
			return 1;
		}else{
			return this.getDuration().compareTo(that.getDuration());
		}
	}


	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public AggregationPeriodUnit getUnit() {
		return unit;
	}

	public void setUnit(AggregationPeriodUnit unit) {
		this.unit = unit;
	}

}
