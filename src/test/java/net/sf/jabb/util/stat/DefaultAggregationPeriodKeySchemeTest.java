/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.SortedSet;

import org.junit.Before;
import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class DefaultAggregationPeriodKeySchemeTest {
	static ZoneId UTC = ZoneId.of("UTC");
	static ZoneId GMT6 = ZoneId.of("GMT-6");
	static ZoneId GMT3 = ZoneId.of("GMT-3");
	
	static String APC_1MIN = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE);
	static String APC_1MONTH = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH);
	static String APC_1YEAR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR);

	static String APC_5MIN = AggregationPeriod.getCodeName(5, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE);
	static String APC_1HOUR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR);
	static String APC_6HOUR = AggregationPeriod.getCodeName(6, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR);
	static String APC_1DAY = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY);
	static String APC_1WEEK_ISO = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_WEEK_ISO);
	static String APC_1WEEK_SUNDAY_START = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_WEEK_SUNDAY_START);

	static String APC_1WEEK_BASED_YEAR_WEEK = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.WEEK_BASED_YEAR_WEEK);
	static String APC_1WEEK_BASED_YEAR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.WEEK_BASED_YEAR);

	AggregationPeriodHierarchy aph;
	HierarchicalAggregationPeriodKeyScheme hapks;
	
	@Before
	public void setup(){
		aph = new AggregationPeriodHierarchy();
		aph.add(APC_5MIN);
			aph.add(APC_5MIN, APC_1HOUR);
				aph.add(APC_1HOUR, APC_6HOUR);
				aph.add(APC_1HOUR, APC_1DAY);
					aph.add(APC_1DAY, APC_1WEEK_ISO);
					aph.add(APC_1DAY, APC_1WEEK_SUNDAY_START);
		
		aph.add(APC_1MIN);
			aph.add(APC_1MIN, APC_1MONTH);
				aph.add(APC_1MONTH, APC_1YEAR);

		aph.add(APC_1WEEK_BASED_YEAR_WEEK);
			aph.add(APC_1WEEK_BASED_YEAR_WEEK, APC_1WEEK_BASED_YEAR);
		
		hapks = new DefaultAggregationPeriodKeyScheme(aph);
	}
	
	@Test
	public void testWithTimeZones() {
		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		ZonedDateTime zdt = ZonedDateTime.of(ldt, GMT6);
		long seconds = zdt.toEpochSecond();
		
		assertEquals(APC_1HOUR + "2015031217", hapks.generateKey(APC_1HOUR, ldt));
		assertEquals(APC_1HOUR + "2015031223", hapks.generateKey(APC_1HOUR, zdt.withZoneSameInstant(UTC)));
		assertEquals(APC_1HOUR + "2015031221", hapks.nextKey(APC_1HOUR + "2015031220", UTC));
		assertEquals(APC_1HOUR + "2015031219", hapks.previousKey(APC_1HOUR + "2015031220", UTC));
		assertEquals(APC_1HOUR + "2015030923", hapks.previousKey(APC_1HOUR + "2015031000", UTC));

		assertEquals(APC_1HOUR + "2015031220", hapks.generateKey(APC_1HOUR, zdt.withZoneSameInstant(GMT3)));

		assertEquals(APC_1HOUR + "2015031217", hapks.generateKey(APC_1HOUR, zdt));
		assertEquals(APC_1HOUR + "2015031217", hapks.generateKey(APC_1HOUR, seconds* 1000L, GMT6));
	}
	
	protected void print(AggregationPeriodKeyScheme scheme, String key){
		System.out.println(key + " -> " + scheme.getStartTime(key) + ", " + scheme.getStartTime(key) + " => " + scheme.nextKey(key, UTC));
	}
	
	@Test
	public void testInHierarchy(){
		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		assertEquals(APC_1MIN + "201503121703", hapks.generateKey(APC_1MIN, ldt));

		assertEquals(APC_1MONTH + "201503", hapks.upperLevelKey(APC_1MIN + "201503121703"));
		assertEquals(APC_1YEAR + "2015", hapks.upperLevelKey(APC_1MONTH + "201503"));
		
		assertEquals(APC_1MONTH + "201501", hapks.firstLowerLevelKey(APC_1YEAR + "2015"));
		assertEquals(APC_1MIN + "201503010000", hapks.firstLowerLevelKey(APC_1MONTH + "201503"));
		
		List<String> keys = hapks.upperLevelKeys(APC_1HOUR + "2015031217");
		assertNotNull(keys);
		assertEquals(2, keys.size());
		assertEquals(APC_6HOUR + "2015031212", keys.get(0));
		assertEquals(APC_1DAY + "20150312", keys.get(1));
		
		testRoundTrip(APC_1MIN + "201503121703");
		testRoundTrip(APC_1MONTH + "201503");
		testRoundTrip(APC_1YEAR + "2015");
		testRoundTrip(APC_1MONTH + "201501");
		testRoundTrip(APC_1MIN + "201503010000");
		testRoundTrip(APC_6HOUR + "2015031212");
		testRoundTrip(APC_1DAY + "20150312");
	}
	
	@Test
	public void testWeeks(){
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200852", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-31T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2009-01-04T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200902", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2009-01-05T10:00")));
		
		assertEquals(APC_1WEEK_BASED_YEAR + "2008", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR + "2009", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR + "2009", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2009-01-04T10:00")));
		
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.nextKey(APC_1WEEK_BASED_YEAR_WEEK + "200852", ZoneId.systemDefault()));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200852", hapks.previousKey(APC_1WEEK_BASED_YEAR_WEEK + "200901", ZoneId.systemDefault()));
		
		
		assertEquals(APC_1WEEK_ISO + "200852", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_ISO + "200853", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_ISO + "200853", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-31T10:00")));
		
		assertEquals(APC_1WEEK_ISO + "200901", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2009-01-01T10:00")));
		assertEquals(APC_1WEEK_SUNDAY_START + "200901", hapks.generateKey(APC_1WEEK_SUNDAY_START, LocalDateTime.parse("2009-01-01T10:00")));

		assertEquals(APC_1WEEK_ISO + "201000", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-01T10:00")));
		assertEquals(APC_1WEEK_SUNDAY_START + "201001", hapks.generateKey(APC_1WEEK_SUNDAY_START, LocalDateTime.parse("2010-01-01T10:00")));

		assertEquals(APC_1WEEK_ISO + "201000", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-03T10:00")));
		assertEquals(APC_1WEEK_ISO + "201001", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-04T10:00")));

		assertEquals(APC_1DAY + "20100101", hapks.generateKey(APC_1DAY, hapks.getStartTime(APC_1WEEK_ISO + "201000")));
		assertEquals(APC_1DAY + "20100101", hapks.generateKey(APC_1DAY, hapks.getStartTime(APC_1WEEK_SUNDAY_START + "201001")));
		
		testRoundTrip(APC_1WEEK_ISO + "200852");
		testRoundTrip(APC_1WEEK_ISO + "200853");
		testRoundTrip(APC_1WEEK_ISO + "201000");
		testRoundTrip(APC_1WEEK_ISO + "201001");

		testRoundTrip(APC_1WEEK_SUNDAY_START + "200852");
		testRoundTrip(APC_1WEEK_SUNDAY_START + "200853");
		testRoundTrip(APC_1WEEK_SUNDAY_START + "201001");
		
		testRoundTrip(APC_1WEEK_BASED_YEAR_WEEK + "200852");
		testRoundTrip(APC_1WEEK_BASED_YEAR_WEEK + "201001");
	}
	
	protected void testRoundTrip(String key){
		assertEquals(key, hapks.nextKey(hapks.previousKey(key, ZoneId.systemDefault()), ZoneId.systemDefault()));
		assertEquals(key, hapks.previousKey(hapks.nextKey(key, ZoneId.systemDefault()), ZoneId.systemDefault()));
	}

}
