/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * The scheme of year, month, day, hour, minute
 * @author James Hu
 *
 */
public class DefaultTimePeriodKeyScheme implements TimePeriodKeyScheme{
	
	protected int step;
	protected ChronoUnit unit;
	protected DateTimeFormatter formatter;
	protected DateTimeFormatter parser;

	public DefaultTimePeriodKeyScheme(int step, ChronoUnit unit, DateTimeFormatter formatter, DateTimeFormatter parser){
		this.step = step;
		this.unit = unit;
		this.formatter = formatter;
		this.parser = parser;
	}
	
	@Override
	public String generateKey(LocalDateTime dateTimeWithoutZone) {
		return formatter.format(dateTimeWithoutZone);
	}

	@Override
	public String generateKey(ZonedDateTime dateTimeWithZone) {
		return formatter.format(dateTimeWithZone);
	}


	@Override
	public LocalDateTime getStartTime(String key) {
		return parser.parse(key, LocalDateTime::from);
	}

	@Override
	public ZonedDateTime getEndTime(String key, ZoneId zone) {
		ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(key), zone);
		ZonedDateTime nextStart = thisStart.plus(step, unit);
		return nextStart;
	}

	@Override
	public String nextKey(String key, ZoneId zone){
		ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(key), zone);
		ZonedDateTime nextStart = thisStart.plus(step, unit);
		return generateKey(nextStart);
	}
	
	@Override
	public String previousKey(String key, ZoneId zone){
		ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(key), zone);
		ZonedDateTime previousStart = thisStart.plus(-step, unit);
		return generateKey(previousStart);
	}

}
