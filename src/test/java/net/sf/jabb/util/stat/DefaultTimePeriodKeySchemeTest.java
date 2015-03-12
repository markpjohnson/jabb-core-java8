/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class DefaultTimePeriodKeySchemeTest {
	static ZoneId UTC = ZoneId.of("UTC");
	static ZoneId GMT6 = ZoneId.of("GMT-6");
	static ZoneId GMT3 = ZoneId.of("GMT-3");

	@Test
	public void test() {
		TimePeriodKeyScheme s1h = new DefaultTimePeriodKeyScheme(1, ChronoUnit.HOURS, 
				DateTimeFormatter.ofPattern("uuuuMMddHH'00'"),
				DateTimeFormatter.ofPattern("uuuuMMddHHmm"));

		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		ZonedDateTime zdt = ZonedDateTime.of(ldt, GMT6);
		long seconds = zdt.toEpochSecond();
		
		assertEquals("201503121700", s1h.generateKey(ldt));
		assertEquals("201503122300", s1h.generateKey(zdt.withZoneSameInstant(UTC)));
		assertEquals("201503122100", s1h.nextKey("201503122000", UTC));
		assertEquals("201503121900", s1h.previousKey("201503122000", UTC));
		assertEquals("201503092300", s1h.previousKey("201503100000", UTC));

		assertEquals("201503122000", s1h.generateKey(zdt.withZoneSameInstant(GMT3)));

		assertEquals("201503121700", s1h.generateKey(zdt));
		assertEquals("201503121700", s1h.generateKey(seconds* 1000L, GMT6));
	}
	
	protected void print(TimePeriodKeyScheme scheme, String key){
		System.out.println(key + " -> " + scheme.getStartTime(key) + ", " + scheme.getStartTime(key) + " => " + scheme.nextKey(key, UTC));
	}

}
