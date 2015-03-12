/**
 * 
 */
package net.sf.jabb.util.stat;

/**
 * Time period key scheme that is in a hierarchical structure.
 * @author James Hu
 *
 */
public interface HierarchicalTimePeriodKeyScheme extends TimePeriodKeyScheme{
	/**
	 * Generate the upper level time period key representing the time period corresponding to the specified key
	 * @param key	the time period key
	 * @return	the upper level key
	 */
	String upperLevelKey(String key);
	
	/**
	 * Generate the time period key representing the fist lower level time period
	 * @param key	the time period key
	 * @return	the lower level key
	 */
	String firstLowerLevelKey(String key);

}
