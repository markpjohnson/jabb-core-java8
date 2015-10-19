/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;


/**
 * Unit of aggregation periods for statistics purposes
 * @author James Hu
 *
 */
public enum AggregationPeriodUnit {
	WEEK_BASED_YEAR('Z', "WEEKBASEDYEAR", ChronoUnit.YEARS), 
		WEEK_BASED_YEAR_WEEK('E', "WEEKBASEDYEARWEEK", ChronoUnit.WEEKS, false, WEEK_BASED_YEAR),
	YEAR('Y', "YEAR", ChronoUnit.YEARS), 
		YEAR_WEEK_ISO('W', "WEEK", ChronoUnit.WEEKS, false, YEAR),
		YEAR_WEEK_SUNDAY_START('U', "WEEKSUNDAYSTART", ChronoUnit.WEEKS, false, YEAR),
		YEAR_MONTH('M', "MONTH", ChronoUnit.MONTHS, false, YEAR, 1, 2, 3, 4, 6, 12), 
			YEAR_MONTH_DAY('D', "DAY", ChronoUnit.DAYS, true, YEAR_MONTH, YEAR_WEEK_ISO, YEAR_WEEK_SUNDAY_START, WEEK_BASED_YEAR_WEEK), 
				YEAR_MONTH_DAY_HOUR('H', "HOUR", ChronoUnit.HOURS, true, YEAR_MONTH_DAY, 1, 2, 3, 4, 6, 8, 12, 24), 
					YEAR_MONTH_DAY_HOUR_MINUTE('N', "MINUTE", ChronoUnit.MINUTES, true, YEAR_MONTH_DAY_HOUR, 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60);

	private Map<AggregationPeriodUnit, int[]> compatibleUpperLevels;
	private boolean compatibilityInheritable;
	private char code;
	private String shortName;
	private TemporalUnit temporalUnit;
	
	static final private Map<Character, AggregationPeriodUnit> codeMapping;
	static final private Map<String, AggregationPeriodUnit> shortNameMapping;
	
	static{
		Map<Character, AggregationPeriodUnit> tmpCodeMapping = new HashMap<Character, AggregationPeriodUnit>();
		Map<String, AggregationPeriodUnit> tmpShortNameMapping = new HashMap<String, AggregationPeriodUnit>();
		for (AggregationPeriodUnit u: AggregationPeriodUnit.values()){
			tmpCodeMapping.put(u.code, u);
			tmpShortNameMapping.put(u.shortName, u);
		}
		codeMapping = ImmutableMap.copyOf(tmpCodeMapping);
		shortNameMapping = ImmutableMap.copyOf(tmpShortNameMapping);
	}
	
	/**
	 * Get the codes of all values
	 * @return	an immutable set containing all the codes
	 */
	static public Set<Character> getAllCodes(){
		return codeMapping.keySet();
	}
	
	/**
	 * Get the short names of all values
	 * @return	an immutable set containing all the short names
	 */
	static public Set<String> getAllShortNames(){
		return shortNameMapping.keySet();
	}
	
	/**
	 * return the enum constant that the input character represents
	 * @param code		the single character code representing the enum constant
	 * @return		the enum constant, or null if not found
	 */
	static public AggregationPeriodUnit parse(char code){
		return codeMapping.get(Character.toUpperCase(code));
	}

	
	/**
	 * return the enum constant that the input string represents
	 * @param string		the string to be parsed, it can be a single character string which is the code, or the same as the enum name, or a short format of the name
	 * @return		the enum constant, or null if not found
	 * @throws  NullPointerException - if the input is null
	 */
	static public AggregationPeriodUnit parse(String string){
		String trimmed = string.trim().toUpperCase();
		if (trimmed.length() == 1){
			return codeMapping.get(trimmed.charAt(0));
		}
		
		if (trimmed.charAt(trimmed.length() - 1) == 'S'){
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}
		AggregationPeriodUnit result;
		result = shortNameMapping.get(trimmed);
		if (result != null){
			return result;
		}
		
		try{
			return valueOf(trimmed);
		}catch(IllegalArgumentException e){
			return null;
		}
	}

	
	/**
	 * Constructor for the up most levels
	 * @param code the single character code to represent this unit
	 * @param shortName the short name to represent this unit
	 * @param temporalUnit  the temporal unit
	 */
	AggregationPeriodUnit(char code, String shortName, TemporalUnit temporalUnit){
		this.code = code;
		this.shortName = shortName;
		this.temporalUnit = temporalUnit;
	}
	
	/**
	 * Constructor for those with only one compatible upper level
	 * @param code the single character code to represent this unit
	 * @param shortName the short name to represent this unit
	 * @param temporalUnit  the temporal unit
	 * @param compatibleUpperLevel	the compatible upper level
	 * @param compatiblePeriods		the compatible number of periods
	 */
	AggregationPeriodUnit(char code, String shortName, TemporalUnit temporalUnit, boolean compatibilityInheritable, AggregationPeriodUnit compatibleUpperLevel, int... compatiblePeriods){
		this.code = code;
		this.shortName = shortName;
		this.temporalUnit = temporalUnit;
		this.compatibilityInheritable = compatibilityInheritable;
		compatibleUpperLevels = new HashMap<>();
		addCompatibleUpperLevel(compatibleUpperLevel, compatiblePeriods);
	}
	
	/**
	 * Constructor for those with only one compatible upper level and only one compatible number of periods which is 1
	 * @param code the single character code to represent this unit
	 * @param shortName the short name to represent this unit
	 * @param temporalUnit  the temporal unit
	 * @param compatibleUpperLevel		the compatible upper level
	 */
	AggregationPeriodUnit(char code, String shortName, TemporalUnit temporalUnit, boolean compatibilityInheritable, AggregationPeriodUnit compatibleUpperLevel){
		this(code, shortName, temporalUnit, compatibilityInheritable, compatibleUpperLevel, 1);
	}
	
	/**
	 * Constructor for those with multiple compatible upper level and all the compatible number of periods are 1
	 * @param code the single character code to represent this unit
	 * @param shortName the short name to represent this unit
	 * @param temporalUnit  the temporal unit
	 * @param compatibleUpperLevels		the compatible upper levels
	 */
	AggregationPeriodUnit(char code, String shortName, TemporalUnit temporalUnit, boolean compatibilityInheritable, AggregationPeriodUnit... compatibleUpperLevels){
		this.code = code;
		this.shortName = shortName;
		this.temporalUnit = temporalUnit;
		this.compatibilityInheritable = compatibilityInheritable;
		this.compatibleUpperLevels = new HashMap<>();
		for (AggregationPeriodUnit compatibleUpperLevel: compatibleUpperLevels){
			addCompatibleUpperLevel(compatibleUpperLevel, 1);
		}
	}
	
	private void addCompatibleUpperLevel(AggregationPeriodUnit unit, int... periods){
		compatibleUpperLevels.put(unit, periods);
		if (unit.compatibleUpperLevels != null && unit.compatibilityInheritable){
			for (Map.Entry<AggregationPeriodUnit, int[]> entry: unit.compatibleUpperLevels.entrySet()){
				Set<Integer> inherited = new HashSet<>();
				for (int upper: entry.getValue()){
					for (int lower: periods){
						inherited.add(upper*lower);
					}
				}
				int[] existing = compatibleUpperLevels.get(entry.getKey());
				if (existing != null){
					for (int i: existing){
						inherited.add(i);
					}
				}
				compatibleUpperLevels.put(entry.getKey(), inherited.stream().sorted().mapToInt(Integer::intValue).toArray());
			}
		}
	}
	
	String getAggregationCompatibilityDetails(AggregationPeriodUnit upperLevelUnit){
		int[] v = compatibleUpperLevels.get(upperLevelUnit);
		if (v == null){
			return null;
		}else{
			StringBuilder sb = new StringBuilder();
			for (int i: v){
				sb.append(i).append(' ');
			}
			if (sb.length() > 0){
				sb.setLength(sb.length() - 1);
			}
			return sb.toString();
		}
	}
	
	String getAggregationCompatibilityDetails(){
		StringBuilder sb = new StringBuilder();
		if (compatibleUpperLevels == null){
			sb.append("top level\n");
		}else{
			compatibleUpperLevels.forEach((k, v)->{
				sb.append(k);
				sb.append(" <- ");
				sb.append(getAggregationCompatibilityDetails(k));
				sb.append('\n');
			});
		}
		return sb.toString();
	}
	
	/**
	 * Determine if a period of this unit can support the aggregation for a period of an upper level
	 * @param thisLevelPeriods		quantity of this level
	 * @param upperLevelUnit		unit of upper level
	 * @param upperLevelPeriods		quantity of upper level
	 * @return	true if it can support, false otherwise
	 */
	public boolean canSupportAggregation(int thisLevelPeriods, AggregationPeriodUnit upperLevelUnit, int upperLevelPeriods){
		int[] compatibleUnitPeriods = this.compatibleUpperLevels.get(upperLevelUnit);
		if (compatibleUnitPeriods == null){
			return false;
		}
		
		for (int i: compatibleUnitPeriods){
			if ((upperLevelPeriods * i) % thisLevelPeriods == 0){
				return true;
			}
		}
		return false;
	}

	public char getCode() {
		return code;
	}

	/**
	 * @return the shortName
	 */
	public String getShortName() {
		return shortName;
	}


	public TemporalUnit getTemporalUnit() {
		return temporalUnit;
	}
}
