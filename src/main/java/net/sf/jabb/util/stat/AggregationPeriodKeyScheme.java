/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * The scheme for generating and parsing keys that identify time periods.
 * Keys are relative to time zone.
 * @author James Hu
 *
 */
public interface AggregationPeriodKeyScheme {

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
	default String nextKey(String key, ZoneId zone){
		return generateKey(getEndTime(key, zone));
	}

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param year			the year
	 * @param month	the month, valid values: [1, 12]
	 * @param dayOfMonth	the day in the month
	 * @param hour		the hour in the day, valid values: [0, 23]
	 * @param minute	the minute in the hour, valid values: [0, 59]
	 * @return	the time period key
	 */
	String generateKey(int year, int month, int dayOfMonth, int hour, int minute);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param dateTimeWithoutZone  the date time
	 * @return	the time period key
	 */
	String generateKey(LocalDateTime dateTimeWithoutZone);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param dateTimeWithZone  the date time
	 * @return	the time period key
	 */
	default String generateKey(ZonedDateTime dateTimeWithZone) {
		return generateKey(dateTimeWithZone.toLocalDateTime());
	}

	/**
	 * Generate time period key from milliseconds since UNIX epoch
	 * @param epochMilli	milliseconds since UNIX epoch
	 * @param zone	the time zone
	 * @return	the time period key
	 */
	default String generateKey(long epochMilli, ZoneId zone){
		return generateKey(ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), zone));
	}
	
	/**
	 * Generate time period key from minutes since UNIX epoch
	 * @param epochMinutes		minutes since UNIX epoch
	 * @param zone				the time zone
	 * @return	the time period key
	 */
	default String generateKey(int epochMinutes, ZoneId zone){
		return generateKey(ZonedDateTime.ofInstant(Instant.ofEpochSecond(60L*epochMinutes), zone));
	}
	

}
