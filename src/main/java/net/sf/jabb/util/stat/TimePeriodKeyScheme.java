/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The scheme for generating and parsing keys that identify time periods
 * @author James Hu
 *
 */
public interface TimePeriodKeyScheme {

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
	 * Generate time period key from milliseconds since epoch
	 * @param epochMilli	milliseconds since epoch
	 * @param zone	the time zone
	 * @return	the time period key
	 */
	default String generateKey(long epochMilli, ZoneId zone){
		return generateKey(ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), zone));
	}

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
	 * Get the end time (exclusive) of the time period represented by the key
	 * @param key	the time period key
	 * @param zone	the time zone
	 * @return	the end time (exclusive) of the time period
	 */
	ZonedDateTime getEndTime(String key, ZoneId zone);

	/**
	 * Get the start time (exclusive) of the time period represented by the key
	 * @param key	the time period key
	 * @return	the start time (inclusive) of the time period
	 */
	LocalDateTime getStartTime(String key);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param dateTimeWithZone  the date time
	 * @return	the time period key
	 */
	String generateKey(ZonedDateTime dateTimeWithZone);

	/**
	 * Generate the key representing the time period that the specified date time falls into
	 * @param dateTimeWithoutZone  the date time
	 * @return	the time period key
	 */
	String generateKey(LocalDateTime dateTimeWithoutZone);

}
