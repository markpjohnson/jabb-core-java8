package net.sf.jabb.util.time;

import static org.junit.Assert.*;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

import net.sf.jabb.util.time.TimeZoneUtility;

import org.junit.Test;

import com.google.common.collect.BiMap;

public class TimeZoneUtilityTest {

	@Test
	public void testUniqueness() {
		BiMap<String, String> idMapping = TimeZoneUtility.getShortenedIdToZoneIdMapping();
		assertEquals(ZoneId.getAvailableZoneIds().size(), idMapping.keySet().size());
		
		Set<String> ids = new HashSet<>();
		for (int i = -64800; i <= 64800; i ++){
			ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(i);
			String zoneId = zoneOffset.getId();
			String shortenedId = TimeZoneUtility.computeShortenedId(zoneId);
			ids.add(shortenedId);
		}
		assertEquals(64800*2 + 1, ids.size());
		
		ids.addAll(idMapping.keySet());
		assertEquals(64800*2+1 + idMapping.keySet().size(), ids.size());
	}
	
	@Test
	public void testLength(){
		BiMap<String, String> idMapping = TimeZoneUtility.getShortenedIdToZoneIdMapping();
		idMapping.keySet().forEach(k->assertEquals(TimeZoneUtility.SHORTENED_ZONE_ID_LENGTH, k.length()));
	}
	
	@Test
	public void printZoneIds(){
		TimeZoneUtility.getShortenedIdToZoneIdMapping().forEach((id, zone)->{
			System.out.println(id + "\t-> " + zone);
		});
	}

}
