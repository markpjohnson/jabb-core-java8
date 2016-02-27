/**
 * 
 */
package net.sf.jabb.util.time;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Utility for time zone related functions.
 * The shortened ids of ZoneIds are 2-character strings containing only a-z characters.
 * @author James Hu
 *
 */
public abstract class TimeZoneUtility {
    /**
     * All possible chars for representing a number as a String
     */
    final static char[] digits = {
        'a' , 'b' ,
        'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
        'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
        'o' , 'p' , 'q' , 'r' , 's' , 't' ,
        'u' , 'v' , 'w' , 'x' , 'y' , 'z'
    };

    
	static protected final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();
	static protected final HashFunction hashToIntFunction = Hashing.murmur3_32();
	static protected final Charset CHARSET_FOR_ENCODING = StandardCharsets.UTF_8;
	static public final int SHORTENED_ZONE_ID_MIN_LENGTH = 1;
	static public final int SHORTENED_ZONE_ID_MAX_LENGTH = 2;

	static public ZoneId UTC = ZoneId.of("UTC");
	
	static protected BiMap<String, String> shortenedIdToZoneIdMapping;
	static protected BiMap<Integer, String> indexToZoneIdMapping;
	
	static{
		Map<String, String> shortenedIdMap = new HashMap<>();
		Map<Integer, String> indexMap = new HashMap<>();
		try(BufferedReader in = new BufferedReader(new InputStreamReader(TimeZoneUtility.class.getResourceAsStream("SortedZoneIds.txt"), StandardCharsets.UTF_8))){
			String line;
			int i = 0;
			while((line = in.readLine()) != null){
				String id = line.trim();
				if (id.length() > 0 && !id.startsWith("#")){	// skip empty lines and comment lines
					if (isValidZoneId(id)){
						String shortenedId = intToAlphaString(i);
						shortenedIdMap.put(shortenedId, id);
						indexMap.put(i, id);
					}else{
						System.err.println("INFO: Unknown ZoneId: " + id);
					}
					i ++;
				}
			}
		} catch(Exception e){
			System.err.println("ERROR: Failed to read sorted ZoneIds from resource");
			e.printStackTrace();
		}
		shortenedIdToZoneIdMapping = ImmutableBiMap.copyOf(shortenedIdMap);
		indexToZoneIdMapping = ImmutableBiMap.copyOf(indexMap);
		
		if (!shortenedIdToZoneIdMapping.values().containsAll(ZoneId.getAvailableZoneIds())){
			Set<String> newZoneIds = new HashSet<>();
			newZoneIds.addAll(ZoneId.getAvailableZoneIds());
			newZoneIds.removeAll(shortenedIdToZoneIdMapping.values());
			System.err.println("WARN: There are new time zones: " + newZoneIds);
		}
	}
	
	/**
	 * Get the mapping between the shortened id and the original id of ZoneId.
	 * Offset-based zone IDs may not be included.
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
	 * Please note that offset-based zone IDs are valid but they may not have corresponding shortened ids.
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
	 * Get the 2-character shortened id of the zone
	 * @param zoneId	id of the time zone
	 * @return	the shortened id, or null if zoneId is not valid
	 */
	static public String toShortenedId(String zoneId){
		return shortenedIdToZoneIdMapping.inverse().get(zoneId);
	}
	
	/**
	 * Get the 2-character shortened id of the zone
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
	
	/**
	 * Get the index of the zone
	 * @param zoneId	id of the time zone
	 * @return	the index, or null if zoneId is not valid
	 */
	static public Integer toIndex(String zoneId){
		return indexToZoneIdMapping.inverse().get(zoneId);
	}
	
	/**
	 * Get the index of the zone
	 * @param zoneId	time zone
	 * @return	the index
	 */
	static public Integer toIndex(ZoneId zoneId){
		return indexToZoneIdMapping.inverse().get(zoneId.getId());
	}
	
	/**
	 * Get the ZoneId corresponding to the index
	 * @param index	the index
	 * @return	the ZoneId, or null if not found
	 */
	static public ZoneId toZoneId(int index){
		String zoneId = indexToZoneIdMapping.get(index);
		return zoneId == null ? null : ZoneId.of(zoneId);
	}
	
	/**
	 * Get the id of the ZoneId corresponding to the index
	 * @param index	the index
	 * @return	the id of the ZoneId, or null if not found
	 */
	static public String toZoneIdId(int index){
		return indexToZoneIdMapping.get(index);
	}
	
	static protected List<String> getSortedZoneIds(){
		Set<String> all = ZoneId.getAvailableZoneIds();
		@SuppressWarnings("unchecked")
		Predicate<String>[] filters = new Predicate[] {
			x->((String)x).startsWith("UTC"),
			x->((String)x).equals("GMT"),
			x->((String)x).equals("Etc/GMT"),
			x->((String)x).startsWith("Etc/GMT-") && ((String)x).length() <= 9,
			x->((String)x).equals("Etc/GMT-10") || ((String)x).equals("Etc/GMT-11"),
			x->((String)x).startsWith("Etc/GMT+") && ((String)x).length() <= 9,
			x->((String)x).startsWith("Etc/GMT+") && ((String)x).length() > 9,
			x->true
		};
		Predicate<String> lastFilter= filters[filters.length - 1];
		for (int i = 0; i < filters.length - 1; i ++){
			lastFilter = lastFilter.and(filters[i].negate());
		}
		filters[filters.length - 1] = lastFilter;
		
		Stream<String> result = Stream.empty();
		for (Predicate<String> f: filters){
			result = Stream.concat(result, all.stream().filter(f).sorted());
		}

		return result.collect(Collectors.toList());
	}
	
	/**
	 * Convert a integer value to a radix-26 encoded string.
	 * The encoding is not standard, it uses only a-z characters
	 * @param i		the integer
	 * @return	the encoding result by the modified radix-36
	 */
	static protected String intToAlphaString(int i){
		int radix = 26;
        char buf[] = new char[33];
        boolean negative = (i < 0);
        int charPos = 32;

        if (!negative) {
            i = -i;
        }

        while (i <= -radix) {
            buf[charPos--] = digits[-(i % radix)];
            i = i / radix;
        }
        buf[charPos] = digits[-i];

        if (negative) {
            buf[--charPos] = '-';
        }

        return new String(buf, charPos, (33 - charPos));
	}
	
}
