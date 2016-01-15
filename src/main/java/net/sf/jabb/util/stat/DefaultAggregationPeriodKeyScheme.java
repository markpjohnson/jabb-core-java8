/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import net.sf.jabb.util.time.TimeZoneUtility;

/**
 * The scheme of year, month, day, hour, minute
 * @author James Hu
 *
 */
public class DefaultAggregationPeriodKeyScheme implements HierarchicalAggregationPeriodKeyScheme, Serializable{
	private static final long serialVersionUID = -3654502940787144075L;

	protected AggregationPeriodHierarchy<?> aph;

	protected DefaultAggregationPeriodKeyScheme(AggregationPeriodHierarchy<?> aggregationPeriodHierarchy){
		this.aph = aggregationPeriodHierarchy;
	}
	
	
	@Override
	public String generateKey(String apCode, int year, int month, int dayOfMonth, int hour, int minute) {
		return generateKey(aph.get(apCode), year, month, dayOfMonth, hour, minute);
	}
	
	@Override
	public String generateKey(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute) {
		return staticGenerateKey(ap, year, month, dayOfMonth, hour, minute);
	}
	
	
	@Override
	public String generateKey(String apCode, LocalDateTime dateTimeWithoutZone) {
		return generateKey(aph.get(apCode), dateTimeWithoutZone);
	}
		
	@Override
	public String generateKey(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone){
		return staticGenerateKey(ap, dateTimeWithoutZone);
	}
	
	static protected String staticGenerateKey(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute) {
		String apCode = ap.getCodeName();
		switch(ap.unit){
			case YEAR:
				return toString(apCode, year - year % ap.amount, 4);
			case YEAR_MONTH:
				return toString(apCode, year*100 + month - month % ap.amount, 6);
			case YEAR_MONTH_DAY:
				return toString(apCode, year*10000 + month * 100 + dayOfMonth, 8);	// amount must be 1
			case YEAR_MONTH_DAY_HOUR:
				return toString(apCode, year*1000000L + month * 10000 + dayOfMonth * 100 + hour - hour % ap.amount, 10);
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				return toString(apCode, year*100000000L + month * 1000000 + dayOfMonth * 10000 + hour * 100 + minute - minute % ap.amount, 12);
			default:
				/*	
				case WEEK_BASED_YEAR:
				case WEEK_BASED_YEAR_WEEK:
				case YEAR_WEEK_ISO:
				case YEAR_WEEK_SUNDAY_START:
				*/
				return staticGenerateKey(ap, LocalDateTime.of(year, month, dayOfMonth, hour, minute));
		}
	}

	static protected String staticGenerateKey(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone){
		String apCode = ap.getCodeName();
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
				year = dateTimeWithoutZone.get(IsoFields.WEEK_BASED_YEAR);
				return toString(apCode, year - year % ap.amount, 4);
			case WEEK_BASED_YEAR_WEEK:
				year = dateTimeWithoutZone.get(IsoFields.WEEK_BASED_YEAR);
				week = dateTimeWithoutZone.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
				return toString(apCode, year*100 + week, 6);		// amount must be 1
			case YEAR_WEEK_ISO:
				year = dateTimeWithoutZone.getYear();
				week = dateTimeWithoutZone.get(WeekFields.ISO.weekOfYear());
				return toString(apCode, year*100 + week, 6);		// amount must be 1
			case YEAR_WEEK_SUNDAY_START:
				year = dateTimeWithoutZone.getYear();
				week = dateTimeWithoutZone.get(WeekFields.SUNDAY_START.weekOfYear());
				return toString(apCode, year*100 + week, 6);		// amount must be 1
			default:
				return staticGenerateKey(ap, dateTimeWithoutZone.getYear(), dateTimeWithoutZone.getMonthValue(), dateTimeWithoutZone.getDayOfMonth(), 
						dateTimeWithoutZone.getHour(), dateTimeWithoutZone.getMinute());
		}
	}
	
	/**
	 * Retrieve the aggregation period information from the key
	 * @param key	the key starts with aggregation period code name
	 * @return		the aggregation period, or null if not found
	 */
	@Override
	public AggregationPeriod retrieveAggregationPeriod(String key){
		int i = endOfAggregationPeriod(key);
		return i > 0 ? AggregationPeriod.parse(key.substring(0, i)) : null;
	}
	
	/**
	 * Separate the part representing AggregationPeriod from the key
	 * @param key	the key starts with aggregation period code name
	 * @return	An array that the first element is the code name of the AggregationPeriod or null if something went wrong, 
	 * 			and the second element is the remaining part of the key
	 */
	@Override
	public String[] separateAggregationPeriod(String key){
		int i = endOfAggregationPeriod(key);
		if ( i > 0){
			return new String[] {key.substring(0, i), key.substring(i)};
		}else{
			return new String[] {null, key};
		}
	}
	
	/**
	 * Find the end position of the aggregation period code
	 * @param key	the key starts with aggregation period code name
	 * @return the next position after the last character of aggregation period code name, or -1 if not found
	 */
	protected int endOfAggregationPeriod(String key){
		int i;
		
		// find the first non-digit which should be the start of the AggregationPeriodUnit code
		for (i = 1; i < key.length(); i ++){
			if (!Character.isDigit(key.charAt(i))){
				// find the next first digit which marks the end of the AggregationPeriodUnit code
				for (;i < key.length(); i ++){
					if (Character.isDigit(key.charAt(i))){
						return i;
					}
				}
			}
		}
		return -1;
	}
	

	/**
	 * Get the start time (inclusive) of the time period represented by the key.
	 * It accepts keys generated with any aggregation period.
	 * The key always marks the start time so there is no time zone information needed as argument.
	 * @param key	the time period key
	 * @return	the start time (inclusive) of the time period. It should be interpreted as in the same time zone in which the key is generated.
	 */
	@Override
	public LocalDateTime getStartTime(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return getStartTime(ap, key);
	}		
		
	static protected LocalDateTime getStartTime(AggregationPeriod ap, String key){
		long k = Long.parseLong(key.substring(ap.getCodeName().length()));
		int i = (int)k;
		StringBuilder sb;
		switch(ap.unit){
			case YEAR:
				return LocalDateTime.of(i, 1, 1, 0, 0);
			case YEAR_MONTH:
				return LocalDateTime.of(i / 100, i % 100, 1, 0, 0);
			case YEAR_MONTH_DAY:
				return LocalDateTime.of(i / 10000, (i % 10000)/100, i % 100, 0, 0);
			case YEAR_MONTH_DAY_HOUR:
				return LocalDateTime.of((int)(k/1000000), (int)(k % 1000000)/10000, (int)(k % 10000) / 100, (int)(k % 100), 0);
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				return LocalDateTime.of((int)(k/100000000L), (int)((k % 100000000L)/1000000), (int)(k % 1000000)/10000, (int)(k % 10000) / 100, (int)(k % 100));
			case WEEK_BASED_YEAR:
				sb = new StringBuilder();
				sb.append(i); // year
				sb.append("-W01-1");
				return LocalDateTime.parse(sb.toString(), DateTimeFormatter.ISO_WEEK_DATE);
			case WEEK_BASED_YEAR_WEEK:
				sb = new StringBuilder();
				sb.append(i/100); // year
				sb.append(toString("-W", i % 100, 2)); // week;
				sb.append("-1");
				return LocalDate.parse(sb.toString(), DateTimeFormatter.ISO_WEEK_DATE).atTime(0, 0);
			case YEAR_WEEK_ISO:
				return LocalDateTime.of(i/100, 1, 1, 0, 0).with(WeekFields.ISO.weekOfYear(), i % 100);
			case YEAR_WEEK_SUNDAY_START:
				return LocalDateTime.of(i/100, 1, 1, 0, 0).with(WeekFields.SUNDAY_START.weekOfYear(), i % 100);
			default:
				throw new IllegalArgumentException("Unknown aggregation period unit: " + ap.unit);
		}
	}

	@Override
	public ZonedDateTime getEndTime(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return getEndTime(ap, key);
	}
	
	static protected ZonedDateTime getEndTime(AggregationPeriod ap, String key) {
		ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key), ap.zone);
		ZonedDateTime nextStart = thisStart.plus(ap.amount, ap.unit.getTemporalUnit());
		return nextStart;
	}

	/**
	 * Iterate along time to find next key
	 * @param ap	the aggregation period
	 * @param key	the key at the start point
	 * @param step	step to move forward (if positive) or backward (if negative)
	 * @param unit	unit of the step
	 * @param zone	the time zone
	 * @return	the first key found that is different form the key at the start point
	 */
	protected static String findNextKey(AggregationPeriod ap, String key, int step, TemporalUnit unit, ZoneId zone){
		for (ZonedDateTime time = ZonedDateTime.of(getStartTime(ap, key), zone).plus(step, unit);; time = time.plus(step, unit)){
			String nextKey = staticGenerateKey(ap, time.toLocalDateTime());
			if (!nextKey.equals(key)){
				return nextKey;
			}
		}
	}

	@Override
	public String previousKey(String key){
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return previousKey(ap, key);
	}
	
	static protected String previousKey(AggregationPeriod ap, String key){
		String apCode;
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
			case YEAR:
				// this is a performance optimization
				apCode = ap.getCodeName();
				year = Integer.parseInt(key.substring(apCode.length()));
				year -= ap.amount;
				return apCode + year;
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				// if the week is the first of the year, we have to iterate through days
				apCode = ap.getCodeName();
				week = Integer.parseInt(key.substring(apCode.length() + 4, key.length()));
				if (week <= 1){
					return findNextKey(ap, key, -1, ChronoUnit.DAYS, ap.zone);
				}
				// else fall down
			case WEEK_BASED_YEAR_WEEK:
				// fall down
			default:
				// it is safe to simply jump to the start of previous period
				ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key), ap.zone);
				ZonedDateTime previousStart = thisStart.plus(-ap.amount, ap.unit.getTemporalUnit());
				return staticGenerateKey(ap, previousStart.toLocalDateTime());
		}
	}
	
	@Override
	public String nextKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return nextKey(ap, key);
	}
	
	static protected String nextKey(AggregationPeriod ap, String key) {
		String apCode;
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
			case YEAR:
				// this is a performance optimization
				apCode = ap.getCodeName();
				year = Integer.parseInt(key.substring(apCode.length()));
				year += ap.amount;
				return apCode + year;
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				// if the week is the first of the year, we have to iterate through days
				apCode = ap.getCodeName();
				week = Integer.parseInt(key.substring(apCode.length() + 4, key.length()));
				if (week >= 51){
					return findNextKey(ap, key, 1, ChronoUnit.DAYS, ap.zone);
				}
				// else fall down
			case WEEK_BASED_YEAR_WEEK:
				// fall down
			default:
				// it is safe to simply jump to the start of previous period
				ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key), ap.zone);
				ZonedDateTime nextStart = thisStart.plus(ap.amount, ap.unit.getTemporalUnit());
				return staticGenerateKey(ap, nextStart.toLocalDateTime());
		}
	}


	@Override
	public String upperLevelKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		Set<AggregationPeriod> uaps = aph.getUpperLevelAggregationPeriods(ap);
		if (uaps.size() > 0){
			AggregationPeriod uap = uaps.iterator().next();
			return generateKey(uap, getStartTime(ap, key));
		}else{
			return null;
		}
	}

	@Override
	public List<String> upperLevelKeys(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		LocalDateTime startTime = getStartTime(ap, key);
		Set<AggregationPeriod> uaps = aph.getUpperLevelAggregationPeriods(ap);
		return uaps.stream().map(p->generateKey(p, startTime)).collect(Collectors.toList());
	}

	@Override
	public String firstLowerLevelKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		AggregationPeriod lap = aph.getLowerLevelAggregationPeriod(ap);
		return generateKey(lap, getStartTime(ap, key));
	}


	@Override
	public String toString(){
		return super.toString();
	}

	/**
	 * Convert a non-negative number to a fixed-length string format. For internal usage.
	 * @param prefix				the prefix to be appended
	 * @param nonNegativeNumber		the number to be converted, must not be negative
	 * @param numberLength			required length of the number in the returned string, 
	 * 								if the number is shorter than this length, it will be left padded with '0's
	 * @return		the string representing the number with possible leading zeros. 
	 * 				Length of the string may be greater than numberLength+prefix.length() if the number is too large to be fitted within numberLength.
	 */
	protected static String toString(String prefix, long nonNegativeNumber, int numberLength){
		String str = Long.toString(nonNegativeNumber);
	    int len = str.length();

	    StringBuilder sb = new StringBuilder();
	    sb.append(prefix);
	    for(int i = numberLength; i > len; i--){
	        sb.append('0');
	    }
	    sb.append(str);
	    return sb.toString();       
	}
	
	/**
	 * Create a hierarchical instance
	 * @param aph	the hierarchy of aggregation periods
	 * @return	the HierarchicalAggregationPeriodKeyScheme
	 */
	static public HierarchicalAggregationPeriodKeyScheme newInstance(AggregationPeriodHierarchy<?> aph){
		return new DefaultAggregationPeriodKeyScheme(aph);
	}

	/**
	 * Create an non-hierarchical instance specific to an aggregation period
	 * @param ap	the aggregation period
	 * @return		a AggregationPeriodKeyScheme specific to the aggregation period 
	 */
	static public AggregationPeriodKeyScheme newInstance(AggregationPeriod ap){
		return new AggregationPeriodKeyScheme(){

			@Override
			public LocalDateTime getStartTime(String key) {
				return DefaultAggregationPeriodKeyScheme.getStartTime(ap, key);
			}

			@Override
			public ZonedDateTime getEndTime(String key) {
				return DefaultAggregationPeriodKeyScheme.getEndTime(ap, key);
			}

			@Override
			public String previousKey(String key) {
				return DefaultAggregationPeriodKeyScheme.previousKey(ap, key);
			}

			@Override
			public String generateKey(int year, int month, int dayOfMonth, int hour, int minute) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKey(ap, year, month, dayOfMonth, hour, minute);
			}

			@Override
			public String generateKey(LocalDateTime dateTimeWithoutZone) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKey(ap, dateTimeWithoutZone);
			}
			
		};
	}

}
