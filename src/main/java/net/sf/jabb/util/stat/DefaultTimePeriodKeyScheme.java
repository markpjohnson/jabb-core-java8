/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The scheme of year, month, day, hour, minute
 * @author James Hu
 *
 */
public class DefaultTimePeriodKeyScheme implements HierarchicalTimePeriodKeyScheme{
	
	protected int step;
	protected ChronoUnit unit;
	protected DateTimeFormatter formatter;
	protected DateTimeFormatter parser;
	
	protected DefaultTimePeriodKeyScheme lowerLevelScheme;
	protected List<HierarchicalTimePeriodKeyScheme> upperLevelSchemes = new ArrayList<>(2);

	public DefaultTimePeriodKeyScheme(int step, ChronoUnit unit, DateTimeFormatter formatter, DateTimeFormatter parser){
		this.step = step;
		this.unit = unit;
		this.formatter = formatter;
		this.parser = parser;
	}
	
	public DefaultTimePeriodKeyScheme(DefaultTimePeriodKeyScheme lowerLevelScheme, int step, ChronoUnit unit, DateTimeFormatter formatter, DateTimeFormatter parser){
		this(step, unit, formatter, parser);
		this.lowerLevelScheme = lowerLevelScheme;
		lowerLevelScheme.upperLevelSchemes.add(this);
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

	@Override
	public String upperLevelKey(String key) {
		HierarchicalTimePeriodKeyScheme upperLevelScheme = getUpperLevelScheme();
		return upperLevelScheme == null? null : upperLevelScheme.generateKey(getStartTime(key));
	}

	@Override
	public List<String> upperLevelKeys(String key) {
		LocalDateTime startTime = getStartTime(key);
		return upperLevelSchemes.stream().map(scheme -> scheme.generateKey(startTime)).collect(Collectors.toList());
	}

	@Override
	public String firstLowerLevelKey(String key) {
		HierarchicalTimePeriodKeyScheme lowerLevelScheme = getLowerLevelScheme();
		return lowerLevelScheme == null ? null : lowerLevelScheme.generateKey(getStartTime(key));
	}

	@Override
	public HierarchicalTimePeriodKeyScheme getUpperLevelScheme() {
		if (upperLevelSchemes.size() > 0){
			return upperLevelSchemes.get(0);
		}else{
			return null;
		}
	}

	@Override
	public List<HierarchicalTimePeriodKeyScheme> getUpperLevelSchemes() {
		return upperLevelSchemes;
	}

	@Override
	public HierarchicalTimePeriodKeyScheme getLowerLevelScheme() {
		return lowerLevelScheme;
	}

	@Override
	public String toString(){
		return formatter.toString() + " - " + step + " " + unit;
	}
}
