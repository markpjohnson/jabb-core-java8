/**
 * 
 */
package net.sf.jabb.util.stat;

import java.util.List;

/**
 * Time period key scheme that is in a hierarchical structure.
 * For any of the key schemes, there is always 0 or 1 lower level scheme, and 0 or n upper level schemes.
 * For any of the keys, the lower level scheme may have more than 1 corresponding keys, 
 * and each of the upper level schemes can have only 1 corresponding key.
 * @author James Hu
 *
 */
public interface HierarchicalTimePeriodKeyScheme extends TimePeriodKeyScheme{
	
	/**
	 * Generate the upper level time period key representing the time period corresponding to the specified key
	 * @param key	the time period key
	 * @return	the upper level key, in the case there are more than one upper level keys, return the first/default one. 
	 * It can be null if this is already the highest level.
	 */
	String upperLevelKey(String key);
	
	/**
	 * In the case that there are more than one upper level definitions, use this method.
	 * @param key	the time period key
	 * @return		all the upper level keys, can be empty if this is already the highest level
	 */
	List<String> upperLevelKeys(String key);
	
	/**
	 * Generate the time period key representing the fist lower level time period
	 * @param key	the time period key
	 * @return	the lower level key
	 */
	String firstLowerLevelKey(String key);
	
	/**
	 * Get the upper level scheme
	 * @return	the default/first upper level scheme, or null if this is already the highest level
	 */
	HierarchicalTimePeriodKeyScheme getUpperLevelScheme();
	
	/**
	 * Get all the upper level schemes
	 * @return	all the upper level schemes, can be empty if this is already the highest level
	 */
	List<HierarchicalTimePeriodKeyScheme> getUpperLevelSchemes();
	
	/**
	 * Get the lower level schemes
	 * @return	the lower level scheme, or null if this is already the lowest level
	 */
	HierarchicalTimePeriodKeyScheme getLowerLevelScheme();
}
