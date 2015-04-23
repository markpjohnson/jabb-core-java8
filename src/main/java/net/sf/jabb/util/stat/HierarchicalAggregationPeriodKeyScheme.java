/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Time period key scheme that is in a hierarchical structure.
 * For any of the key schemes, there is always 0 or 1 lower level scheme, and 0 or n upper level schemes.
 * For any of the keys, the lower level scheme may have more than 1 corresponding keys, 
 * and each of the upper level schemes can have only 1 corresponding key.
 * @author James Hu
 *
 */
public interface HierarchicalAggregationPeriodKeyScheme{
	
	/**
	 * To check if a key is valid
	 * @param key	the key
	 * @return	true if it is a valid key, false if it is not
	 */
	default boolean isValid(String key){
		try{
			getStartTime(key);
			return true;
		}catch(Exception e){
			return false;
		}
	}

	/**
	 * Get the start time (inclusive) of the time period represented by the key.
	 * The key always marks the start time so there is no time zone information needed as argument.
	 * @param key	the time period key
	 * @return	the start time (inclusive) of the time period. It should be interpreted as in the same time zone in which the key is generated.
	 */
	LocalDateTime getStartTime(String key);

	/**
	 * Get the end time (exclusive) of the time period represented by the key.
	 * The key always marks the start time but in order to calculate the end time, time zone information is needed.
	 * @param key	the time period key
	 * @param zone	the time zone in which the end time will be calculated
	 * @return	the end time (exclusive) of the time period
	 */
	ZonedDateTime getEndTime(String key, ZoneId zone);

	/**
	 * Generate the key representing the previous time period of a specified key
	 * @param key	the key for which the key for previous time period will be generated
	 * @param zone	the time zone
	 * @return	the key identifying the previous time period
	 */
	String previousKey(String key, ZoneId zone);

	/**
	 * Generate the key representing the next time period of a specified key
	 * @param key	the key for which the key for next time period will be generated
	 * @param zone	the time zone
	 * @return the key identifying the next time period
	 */
	String nextKey(String key, ZoneId zone);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriodCodeName		the code name of the aggregation period for which the key will be generated
	 * @param year			the year
	 * @param month	the month, valid values: [1, 12]
	 * @param dayOfMonth	the day in the month
	 * @param hour		the hour in the day, valid values: [0, 23]
	 * @param minute	the minute in the hour, valid values: [0, 59]
	 * @return	the time period key
	 */
	String generateKey(String aggregationPeriodCodeName, int year, int month, int dayOfMonth, int hour, int minute);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriod		the aggregation period for which the key will be generated
	 * @param year			the year
	 * @param month	the month, valid values: [1, 12]
	 * @param dayOfMonth	the day in the month
	 * @param hour		the hour in the day, valid values: [0, 23]
	 * @param minute	the minute in the hour, valid values: [0, 59]
	 * @return	the time period key
	 */
	default String generateKey(AggregationPeriod aggregationPeriod, int year, int month, int dayOfMonth, int hour, int minute){
		return generateKey(aggregationPeriod.getCodeName(), year, month, dayOfMonth, hour, minute);
	}
	
	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriodCodeName		the code name of the aggregation period for which the key will be generated
	 * @param dateTimeWithoutZone  the date time
	 * @return	the time period key
	 */
	String generateKey(String aggregationPeriodCodeName, LocalDateTime dateTimeWithoutZone);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriod		the aggregation period for which the key will be generated
	 * @param dateTimeWithoutZone  the date time
	 * @return	the time period key
	 */
	default String generateKey(AggregationPeriod aggregationPeriod, LocalDateTime dateTimeWithoutZone){
		return generateKey(aggregationPeriod.getCodeName(), dateTimeWithoutZone);
	}

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriodCodeName		the code name of the aggregation period for which the key will be generated
	 * @param dateTimeWithZone  the date time
	 * @return	the time period key
	 */
	default String generateKey(String aggregationPeriodCodeName, ZonedDateTime dateTimeWithZone) {
		return generateKey(aggregationPeriodCodeName, dateTimeWithZone.toLocalDateTime());
	}

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param aggregationPeriod		the aggregation period for which the key will be generated
	 * @param dateTimeWithZone  the date time
	 * @return	the time period key
	 */
	default String generateKey(AggregationPeriod aggregationPeriod, ZonedDateTime dateTimeWithZone) {
		return generateKey(aggregationPeriod.getCodeName(), dateTimeWithZone.toLocalDateTime());
	}

	/**
	 * Generate time period key from milliseconds since UNIX epoch
	 * @param aggregationPeriodCodeName		the code name of the aggregation period for which the key will be generated
	 * @param epochMilli	milliseconds since UNIX epoch
	 * @param zone	the time zone
	 * @return	the time period key
	 */
	default String generateKey(String aggregationPeriodCodeName, long epochMilli, ZoneId zone){
		return generateKey(aggregationPeriodCodeName, ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), zone));
	}
	
	/**
	 * Generate time period key from milliseconds since UNIX epoch
	 * @param aggregationPeriod		the aggregation period for which the key will be generated
	 * @param epochMilli	milliseconds since UNIX epoch
	 * @param zone	the time zone
	 * @return	the time period key
	 */
	default String generateKey(AggregationPeriod aggregationPeriod, long epochMilli, ZoneId zone){
		return generateKey(aggregationPeriod.getCodeName(), ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), zone));
	}
	
	/**
	 * Generate time period key from minutes since UNIX epoch
	 * @param aggregationPeriodCodeName		the code name of the aggregation period for which the key will be generated
	 * @param epochMinutes		minutes since UNIX epoch
	 * @param zone				the time zone
	 * @return	the time period key
	 */
	default String generateKey(String aggregationPeriodCodeName, int epochMinutes, ZoneId zone){
		return generateKey(aggregationPeriodCodeName, ZonedDateTime.ofInstant(Instant.ofEpochSecond(60L*epochMinutes), zone));
	}

	/**
	 * Generate time period key from minutes since UNIX epoch
	 * @param aggregationPeriod		the aggregation period for which the key will be generated
	 * @param epochMinutes		minutes since UNIX epoch
	 * @param zone				the time zone
	 * @return	the time period key
	 */
	default String generateKey(AggregationPeriod aggregationPeriod, int epochMinutes, ZoneId zone){
		return generateKey(aggregationPeriod.getCodeName(), ZonedDateTime.ofInstant(Instant.ofEpochSecond(60L*epochMinutes), zone));
	}

	/**
	 * Generate the upper level time period key representing the time period corresponding to the specified key
	 * @param key	the time period key
	 * @return	the upper level key, in the case there are more than one upper level keys, return the first/default one. 
	 * It can be null if this is already the highest level.
	 */
	String upperLevelKey(String key);
	
	/**
	 * In the case that there are more than one upper level definitions, use this method.
	 * @param key	the time period key
	 * @return		all the upper level keys, can be empty if this is already the highest level
	 */
	List<String> upperLevelKeys(String key);
	
	/**
	 * Generate the time period key representing the fist lower level time period
	 * @param key	the time period key
	 * @return	the lower level key
	 */
	String firstLowerLevelKey(String key);
	
}
