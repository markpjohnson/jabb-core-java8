/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import java.time.ZoneId;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AggregationPeriodTest {

	@Test
	public void testParsing() {
		AggregationPeriod.parse("2 years");
		assertEquals(AggregationPeriod.parse("2y"), AggregationPeriod.parse("2 year"));
		assertEquals(AggregationPeriod.parse("2y"), AggregationPeriod.parse("2 years"));
		
		assertEquals(AggregationPeriod.parse("1h", ZoneId.of("Pacific/Apia")), AggregationPeriod.parse("to1H"));
		assertEquals(AggregationPeriod.parse("1h", ZoneId.of("Pacific/Apia")), AggregationPeriod.parse("to 1 H"));
		assertEquals(AggregationPeriod.parse("1h", ZoneId.of("Pacific/Apia")), AggregationPeriod.parse("to 1 Hour"));
		assertEquals(AggregationPeriod.parse("1h", ZoneId.of("Pacific/Apia")), AggregationPeriod.parse("to 1 Hours"));
		assertEquals(AggregationPeriod.parse("1h", ZoneId.of("Pacific/Apia")), AggregationPeriod.parse("to 1 YEAR_MONTH_DAY_HOUR"));
		
		assertEquals(AggregationPeriod.parse("2d"), AggregationPeriod.parse("2 days"));
		assertEquals(AggregationPeriod.parse("2d"), AggregationPeriod.parse("2 day"));
		assertEquals(AggregationPeriod.parse("2d"), AggregationPeriod.parse("2 YEAR_MONTH_DAY"));
		
		assertEquals(AggregationPeriod.parse("2E"), AggregationPeriod.parse("2 WeekBasedYearWeek"));
		assertEquals(AggregationPeriod.parse("2E"), AggregationPeriod.parse("2 WEEK_BASED_YEAR_WEEK"));
	}

}
