/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class AggregationPeriodUnitTest {

	
	//@Test
	public void displayDetails() {
		for (AggregationPeriodUnit apu: AggregationPeriodUnit.values()){
			System.out.println("=== " + apu + " ===");
			System.out.println(apu.getAggregationCompatibilityDetails());
		}
	}

	@Test
	public void testCompatibilities() {
		assertEquals("1 2 3 4 5 6 8 9 10 12 15 16 18 20 24 30 32 36 40 45 48 60 72 80 90 96 120 144 160 180 240 288 360 480 720 1440", 
				AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE.getAggregationCompatibilityDetails(AggregationPeriodUnit.YEAR_WEEK_SUNDAY_START));
		assertEquals("1 2 3 4 5 6 10 12 15 20 30 60", 
				AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE.getAggregationCompatibilityDetails(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR));

		assertEquals("1 2 3 4 6 8 12 24", 
				AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR.getAggregationCompatibilityDetails(AggregationPeriodUnit.YEAR_MONTH_DAY));

		assertEquals("1 2 3 4 6 12", 
				AggregationPeriodUnit.YEAR_MONTH.getAggregationCompatibilityDetails(AggregationPeriodUnit.YEAR));

		assertEquals("1", 
				AggregationPeriodUnit.YEAR_WEEK_ISO.getAggregationCompatibilityDetails(AggregationPeriodUnit.YEAR));
		
		assertTrue(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR.canSupportAggregation(6, AggregationPeriodUnit.YEAR_MONTH_DAY, 1));
		assertFalse(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR.canSupportAggregation(5, AggregationPeriodUnit.YEAR_MONTH_DAY, 1));
		assertTrue(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR.canSupportAggregation(5, AggregationPeriodUnit.YEAR_MONTH_DAY, 5));
	}

	@Test
	public void testParsing(){
		assertEquals(AggregationPeriodUnit.YEAR, AggregationPeriodUnit.parse("year"));
		assertEquals(AggregationPeriodUnit.YEAR, AggregationPeriodUnit.parse("years"));
		assertEquals(AggregationPeriodUnit.YEAR, AggregationPeriodUnit.parse("y"));

		assertEquals(AggregationPeriodUnit.WEEK_BASED_YEAR, AggregationPeriodUnit.parse("WEEK_BASED_YEAR"));
		assertEquals(AggregationPeriodUnit.WEEK_BASED_YEAR, AggregationPeriodUnit.parse("WeekBasedYear"));
		assertEquals(AggregationPeriodUnit.WEEK_BASED_YEAR, AggregationPeriodUnit.parse("Z"));

		assertEquals(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, AggregationPeriodUnit.parse("YEAR_MONTH_DAY_HOUR_MINUTE"));
		assertEquals(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, AggregationPeriodUnit.parse("minutes"));
		assertEquals(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, AggregationPeriodUnit.parse("minute"));
		assertEquals(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, AggregationPeriodUnit.parse("n"));
	}
}
