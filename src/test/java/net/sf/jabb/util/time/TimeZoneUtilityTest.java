package net.sf.jabb.util.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.BiMap;

public class TimeZoneUtilityTest {

	@Test
	public void testUniqueness() {
		BiMap<String, String> idMapping = TimeZoneUtility.getShortenedIdToZoneIdMapping();
		assertEquals(ZoneId.getAvailableZoneIds().size(), idMapping.keySet().size());
		
		/* we don't support translation of offseted zones any more
		Set<String> ids = new HashSet<>();
		for (int i = -64800; i <= 64800; i ++){
			ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(i);
			String zoneId = zoneOffset.getId();
			System.out.println(zoneId);
			//String shortenedId = TimeZoneUtility.computeShortenedId(zoneId);
			//ids.add(shortenedId);
		}
		assertEquals(64800*2 + 1, ids.size());
		
		ids.addAll(idMapping.keySet());
		assertEquals(64800*2+1 + idMapping.keySet().size(), ids.size());
		*/
	}
	
	@Test
	public void testLength(){
		BiMap<String, String> idMapping = TimeZoneUtility.getShortenedIdToZoneIdMapping();
		//idMapping.keySet().forEach(k->assertEquals(TimeZoneUtility.SHORTENED_ZONE_ID_MAX_LENGTH, k.length()));
		idMapping.keySet().forEach(k->{
			assertTrue(k.length() >= TimeZoneUtility.SHORTENED_ZONE_ID_MIN_LENGTH);
			assertTrue(k.length() <= TimeZoneUtility.SHORTENED_ZONE_ID_MAX_LENGTH);
		});
	}
	
	@Test
	public void printZoneIds(){
		TimeZoneUtility.getShortenedIdToZoneIdMapping().forEach((id, zone)->{
			//System.out.println(id + "\t-> " + zone);
		});
		//System.out.println(TimeZoneUtility.getShortenedIdToZoneIdMapping().size());
	}

	@Test
	public void testSorting(){
		assertEquals(0, Integer.parseInt("00", Character.MAX_RADIX));
		List<String> sorted = TimeZoneUtility.getSortedZoneIds();
		int i = 0;
		for (String id: sorted){
			System.out.println("" + i + "\t" + TimeZoneUtility.intToAlphaString(i) + "\t" + id);
			i++;
		}
		assertEquals(sorted.size(), sorted.stream().collect(Collectors.toSet()).size());
		assertEquals(ZoneId.getAvailableZoneIds().size(), sorted.stream().collect(Collectors.toSet()).size());
	}
}
