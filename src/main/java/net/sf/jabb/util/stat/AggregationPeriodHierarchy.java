/**
 * 
 */
package net.sf.jabb.util.stat;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The forrest structure of AggregationPeriod nodes
 * @author James Hu
 *
 */
public class AggregationPeriodHierarchy {
	protected Map<String, AggregationPeriodNode> codeMapping = new HashMap<>();
	protected Set<AggregationPeriodNode> rootNodes = new HashSet<>();
	protected SortedSet<AggregationPeriod> roots = new TreeSet<>();

	
	static class AggregationPeriodNode{
		AggregationPeriod aggregationPeriod;
		AggregationPeriodNode lowerLevelNode;
		Set<AggregationPeriodNode> upperLevelNodes;
		SortedSet<AggregationPeriod> upperLevels;
		
		AggregationPeriodNode(AggregationPeriod aggregationPeriod){
			this.aggregationPeriod = aggregationPeriod;
		}
		
		void addUpperLevel(AggregationPeriodNode upperLevel){
			if (upperLevelNodes == null){
				upperLevelNodes = new HashSet<>();
				upperLevels = new TreeSet<>();
			}
			upperLevelNodes.add(upperLevel);
			upperLevels.add(upperLevel.aggregationPeriod);
			upperLevel.lowerLevelNode = this;
		}
	}

	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param aggregationPeriod		the root - lowest level in the aggregation hierarchy
	 * @return		true if successfully added, false if such a root already exists
	 */
	public boolean add(AggregationPeriod aggregationPeriod){
		AggregationPeriodNode root = new AggregationPeriodNode(aggregationPeriod);
		String code = root.aggregationPeriod.getCodeName();
		if (codeMapping.putIfAbsent(code, root) == null){
			rootNodes.add(root);
			roots.add(aggregationPeriod);
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param codeName	code name of the root - lowest level in the aggregation hierarchy
	 * @return		true if successfully added, false if such a root already exists
	 */
	public boolean add(String codeName){
		return add(AggregationPeriod.parse(codeName));
	}
	
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param amount	amount of the aggregation period
	 * @param unit		unit of the aggregation period
	 * @return	true if successfully added, false if such a root already exists
	 */
	public boolean add(int amount, AggregationPeriodUnit unit){
		return add(new AggregationPeriod(amount, unit));
	}

	
	/**
	 * Add a non-root aggregation period
	 * @param base			the lower level aggregation period that the upper level one one is based on. 
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(AggregationPeriod base, AggregationPeriod upperLevel){
		return add(base.getCodeName(), upperLevel);
	}
		
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, AggregationPeriod upperLevel){
		AggregationPeriodNode parent = codeMapping.get(baseCodeName);
		if (parent == null){
			return false;
		}

		AggregationPeriodNode child = new AggregationPeriodNode(upperLevel);
		String code = upperLevel.getCodeName();
		if (codeMapping.putIfAbsent(code, child) == null){
			parent.addUpperLevel(child);
			return true;
		}else{
			return false;
		}
		
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param amount	amount of the aggregation period
	 * @param unit		unit of the aggregation period
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, int amount, AggregationPeriodUnit unit){
		return add(baseCodeName, new AggregationPeriod(amount, unit));
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param codeName		code name of the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, String codeName){
		return add(baseCodeName, AggregationPeriod.parse(codeName));
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseAmount	amount of the lower level aggregation period that the upper level one one is based on.
	 * @param baseUnit		unit of the lower level aggregation period that the upper level one one is based on.
	 * @param amount	amount of the aggregation period that will be added
	 * @param unit		unit of the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(int baseAmount, AggregationPeriodUnit baseUnit, int amount, AggregationPeriodUnit unit){
		return add(AggregationPeriod.getCodeName(baseAmount, baseUnit), new AggregationPeriod(amount, unit));
	}
	
	/**
	 * Get all roots - aggregation periods that are of the lowest level.
	 * @return	the roots
	 */
	public SortedSet<AggregationPeriod> getRoots(){
		return roots;
	}
	
	/**
	 * Get the aggregation period in the hierarchy with the specified code name
	 * @param code	the code name
	 * @return	the matching AggregationPeriod found in the hierarchy, or null if not found
	 */
	public AggregationPeriod get(String code){
		AggregationPeriodNode node = codeMapping.get(code);
		return node == null ? null : node.aggregationPeriod;
	}
	
	/**
	 * Get the lower level aggregation period of an aggregation period in the hierarchy
	 * @param code	the code name of the aggregation period for which the lower level aggregation period needs to be returned
	 * @return the lower level aggregation period, or null if there is no aggregation period matching the code name or if the one matching the code name is a root
	 */
	public AggregationPeriod getLowerLevelAggregationPeriod(String code){
		AggregationPeriodNode node = codeMapping.get(code);
		return node == null ? null : 
			(node.lowerLevelNode == null ? null : node.lowerLevelNode.aggregationPeriod);
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy
	 * @param code	the code name of the aggregation period for which the upper level aggregation periods need to be returned
	 * @return the upper level aggregation periods, it will not be null be may be empty.
	 */
	public SortedSet<AggregationPeriod> getUpperLevelAggregationPeriods(String code){
		AggregationPeriodNode node = codeMapping.get(code);
		SortedSet<AggregationPeriod> result = node == null ? null : 
			(node.upperLevels == null ? null : node.upperLevels);
		return result == null ? Collections.emptySortedSet() : result;
	}
	
	/**
	 * Get the lower level aggregation period of an aggregation period in the hierarchy
	 * @param aggregationPeriod	the aggregation period for which the lower level aggregation period needs to be returned
	 * @return	the lower level aggregation period, or null if the specified aggregation period is a root
	 */
	public AggregationPeriod getLowerLevelAggregationPeriod(AggregationPeriod aggregationPeriod){
		return getLowerLevelAggregationPeriod(aggregationPeriod.getCodeName());
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy
	 * @param aggregationPeriod	the aggregation period for which the upper level aggregation periods need to be returned
	 * @return	the upper level aggregation periods, it will not be null but may be empty.
	 */
	public SortedSet<AggregationPeriod> getUpperLevelAggregationPeriods(AggregationPeriod aggregationPeriod){
		return getUpperLevelAggregationPeriods(aggregationPeriod.getCodeName());
	}
	
	
}
