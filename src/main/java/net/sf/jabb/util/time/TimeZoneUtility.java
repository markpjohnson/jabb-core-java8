/**
 * 
 */
package net.sf.jabb.util.time;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Utility for time zone related functions
 * @author James Hu
 *
 */
public abstract class TimeZoneUtility {
	static protected final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();
	static protected final HashFunction hashToIntFunction = Hashing.murmur3_32();
	static protected final Charset CHARSET_FOR_ENCODING = StandardCharsets.UTF_8;
	static public final int SHORTENED_ZONE_ID_LENGTH = 6;

	static public ZoneId UTC = ZoneId.of("UTC");
	
	static protected BiMap<String, String> shortenedIdToZoneIdMapping;
	
	static{
		shortenedIdToZoneIdMapping = ImmutableBiMap.copyOf(
				ZoneId.getAvailableZoneIds().stream().collect(Collectors.toMap(zoneId -> computeShortenedId((String) zoneId), Function.identity())));
	}
	
	/**
	 * Get the mapping between the shortened id and the original id of ZoneId.
	 * Offset-based zone IDs are not included.
	 * @return	the mapping of (shortened id - original id)
	 */
	public static BiMap<String, String> getShortenedIdToZoneIdMapping() {
		return shortenedIdToZoneIdMapping;
	}
	
	/**
	 * Check to see if the shortened id is valid
	 * @param shortenedId	the shortened id to be tested
	 * @return	true if it is valid, false otherwise
	 */
	public static boolean isValidShortenedId(String shortenedId){
		return shortenedIdToZoneIdMapping.containsKey(shortenedId);
	}
	
	/**
	 * Check to see if the zone id is valid. 
	 * Please not that offset-based zone IDs are valid but they don't have corresponding shortened ids.
	 * @param zoneId	the zone id
	 * @return	true if it is valid, false otherwise
	 */
	public static boolean isValidZoneId(String zoneId){
		try{
			ZoneId.of(zoneId);
			return true;
		}catch(Exception e){
			return false;
		}
	}

	/**
	 * Compute the shortened id of a ZoneId. This method can be applied to ZoneOffset as well.
	 * @param zoneId	id of the ZoneId
	 * @return	the shortened id
	 */
	public static String computeShortenedId(String zoneId){
		return base64.encodeToString(hashToIntFunction.hashString(zoneId, CHARSET_FOR_ENCODING).asBytes());
	}
	
	/**
	 * Get the 6-character shortened id of the zone
	 * @param zoneId	id of the time zone
	 * @return	the shortened id, or null if zoneId is not valid
	 */
	static public String toShortenedId(String zoneId){
		return shortenedIdToZoneIdMapping.inverse().get(zoneId);
	}
	
	/**
	 * Get the 6-character shortened id of the zone
	 * @param zoneId	time zone
	 * @return	the shortened id
	 */
	static public String toShortenedId(ZoneId zoneId){
		return shortenedIdToZoneIdMapping.inverse().get(zoneId.getId());
	}
	
	/**
	 * Get the ZoneId corresponding to the shortened id
	 * @param shortenedId	the shortened id
	 * @return	the ZoneId, or null if not found
	 */
	static public ZoneId toZoneId(String shortenedId){
		String zoneId = shortenedIdToZoneIdMapping.get(shortenedId);
		return zoneId == null ? null : ZoneId.of(zoneId);
	}
	
	/**
	 * Get the id of the ZoneId corresponding to the shortened id
	 * @param shortenedId	the shortened id
	 * @return	the id of the ZoneId, or null if not found
	 */
	static public String toZoneIdId(String shortenedId){
		return shortenedIdToZoneIdMapping.get(shortenedId);
	}
	
}
