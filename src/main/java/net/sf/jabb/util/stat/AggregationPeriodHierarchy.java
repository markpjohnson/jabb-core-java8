/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The forest structure of AggregationPeriod nodes.
 * Each node can have an attachment object.
 * @author James Hu
 *
 * @param <T> type of attachments
 */
public class AggregationPeriodHierarchy<T> implements Serializable{
	private static final long serialVersionUID = 8664232464718744888L;

	protected Map<String, AggregationPeriodNode<T>> codeMapping = new HashMap<>();
	protected SortedMap<AggregationPeriod, AggregationPeriodAndAttachment<T>> roots = new TreeMap<>();

	
	static class AggregationPeriodNode<NT> implements Serializable{
		private static final long serialVersionUID = 4925692035577355240L;

		AggregationPeriodAndAttachment<NT> aggregationPeriodAndAttachment;
		AggregationPeriodNode<NT> lowerLevelNode;
		Set<AggregationPeriodNode<NT>> upperLevelNodes;
		SortedMap<AggregationPeriod, AggregationPeriodAndAttachment<NT>> upperLevels;

		AggregationPeriodNode(AggregationPeriodAndAttachment<NT> aggregationPeriodAndAttachment){
			this.aggregationPeriodAndAttachment = aggregationPeriodAndAttachment;
		}

		AggregationPeriodNode(AggregationPeriod aggregationPeriod, NT attachment){
			this.aggregationPeriodAndAttachment = new AggregationPeriodAndAttachment<NT>(aggregationPeriod, attachment);
		}
		
		void addUpperLevel(AggregationPeriodNode<NT> upperLevel){
			if (upperLevelNodes == null){
				upperLevelNodes = new HashSet<>();
				upperLevels = new TreeMap<>();
			}
			upperLevelNodes.add(upperLevel);
			upperLevels.put(upperLevel.aggregationPeriodAndAttachment.getAggregationPeriod(), upperLevel.aggregationPeriodAndAttachment);
			upperLevel.lowerLevelNode = this;
		}
	}

	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param aggregationPeriod		the root - lowest level in the aggregation hierarchy
	 * @return		true if successfully added, false if such a root already exists
	 */
	public boolean add(AggregationPeriod aggregationPeriod){
		return add(aggregationPeriod, (T)null);
	}
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param aggregationPeriod		the root - lowest level in the aggregation hierarchy
	 * @param attachment		the attachment object
	 * @return		true if successfully added, false if such a root already exists
	 */
	public boolean add(AggregationPeriod aggregationPeriod, T attachment){
		AggregationPeriodNode<T> root = new AggregationPeriodNode<T>(aggregationPeriod, attachment);
		String code = root.aggregationPeriodAndAttachment.getAggregationPeriod().getCodeName();
		if (codeMapping.putIfAbsent(code, root) == null){
			roots.put(aggregationPeriod, new AggregationPeriodAndAttachment<T>(aggregationPeriod, attachment));
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
		return add(codeName, (T)null);
	}
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param codeName	code name of the root - lowest level in the aggregation hierarchy
	 * @param attachment		the attachment object
	 * @return		true if successfully added, false if such a root already exists
	 */
	public boolean add(String codeName, T attachment){
		return add(AggregationPeriod.parse(codeName), attachment);
	}
	
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param amount	amount of the aggregation period
	 * @param unit		unit of the aggregation period
	 * @return	true if successfully added, false if such a root already exists
	 */
	public boolean add(int amount, AggregationPeriodUnit unit){
		return add(amount, unit, (T)null);
	}
	/**
	 * Add a root which represents the lowest level aggregation period in the aggregation hierarchy
	 * @param amount	amount of the aggregation period
	 * @param unit		unit of the aggregation period
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if such a root already exists
	 */
	public boolean add(int amount, AggregationPeriodUnit unit, T attachment){
		return add(new AggregationPeriod(amount, unit), attachment);
	}

	/**
	 * Add a non-root aggregation period
	 * @param base			the lower level aggregation period that the upper level one one is based on. 
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(AggregationPeriod base, AggregationPeriod upperLevel){
		return add(base, upperLevel, (T)null);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param base			the lower level aggregation period that the upper level one one is based on. 
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(AggregationPeriod base, AggregationPeriod upperLevel, T attachment){
		return add(base.getCodeName(), upperLevel, attachment);
	}
		
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, AggregationPeriod upperLevel){
		return add(baseCodeName, upperLevel, (T)null);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param upperLevel	the aggregation period that will be added
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, AggregationPeriod upperLevel, T attachment){
		AggregationPeriodNode<T> parent = codeMapping.get(baseCodeName);
		if (parent == null){
			return false;
		}

		AggregationPeriodNode<T> child = new AggregationPeriodNode<T>(upperLevel, attachment);
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
		return add(baseCodeName, amount, unit, (T)null);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param amount	amount of the aggregation period
	 * @param unit		unit of the aggregation period
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, int amount, AggregationPeriodUnit unit, T attachment){
		return add(baseCodeName, new AggregationPeriod(amount, unit), attachment);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param codeName		code name of the aggregation period that will be added
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, String codeName){
		return add(baseCodeName, codeName, (T)null);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseCodeName	the code name of the lower level aggregation period that the upper level one one is based on.
	 * 						It must already exist in the hierarchy
	 * @param codeName		code name of the aggregation period that will be added
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(String baseCodeName, String codeName, T attachment){
		return add(baseCodeName, AggregationPeriod.parse(codeName), attachment);
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
		return add(baseAmount, baseUnit, amount, unit, (T)null);
	}
	
	/**
	 * Add a non-root aggregation period
	 * @param baseAmount	amount of the lower level aggregation period that the upper level one one is based on.
	 * @param baseUnit		unit of the lower level aggregation period that the upper level one one is based on.
	 * @param amount	amount of the aggregation period that will be added
	 * @param unit		unit of the aggregation period that will be added
	 * @param attachment		the attachment object
	 * @return	true if successfully added, false if the upper level aggregation period already exists or if the base does not exist
	 */
	public boolean add(int baseAmount, AggregationPeriodUnit baseUnit, int amount, AggregationPeriodUnit unit, T attachment){
		return add(AggregationPeriod.getCodeName(baseAmount, baseUnit), new AggregationPeriod(amount, unit), attachment);
	}
	
	/**
	 * Get all roots - aggregation periods that are of the lowest level.
	 * The result is sorted.
	 * @return	the roots
	 */
	public Set<AggregationPeriod> getRoots(){
		return roots.keySet();
	}
	
	/**
	 * Get all roots with attachments - aggregation periods that are of the lowest level.
	 * The result is sorted.
	 * @return	the roots
	 */
	public Collection<AggregationPeriodAndAttachment<T>> getRootsWithAttachments(){
		return roots.values();
	}
	
	/**
	 * Get the aggregation period in the hierarchy with the specified code name
	 * @param code	the code name
	 * @return	the matching AggregationPeriod found in the hierarchy, or null if not found
	 */
	public AggregationPeriod get(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		return node == null ? null : node.aggregationPeriodAndAttachment.getAggregationPeriod();
	}
	
	/**
	 * Get the aggregation period in the hierarchy with the specified code name, with attachment
	 * @param code	the code name
	 * @return	the matching AggregationPeriod found in the hierarchy including attachment, or null if not found
	 */
	public AggregationPeriodAndAttachment<T> getWithAttachment(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		return node == null ? null : node.aggregationPeriodAndAttachment;
	}
	
	/**
	 * Get the lower level aggregation period of an aggregation period in the hierarchy
	 * @param code	the code name of the aggregation period for which the lower level aggregation period needs to be returned
	 * @return the lower level aggregation period, or null if there is no aggregation period matching the code name or if the one matching the code name is a root
	 */
	public AggregationPeriod getLowerLevelAggregationPeriod(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		return node == null ? null : 
			(node.lowerLevelNode == null ? null : node.lowerLevelNode.aggregationPeriodAndAttachment.getAggregationPeriod());
	}
	
	/**
	 * Get the lower level aggregation period of an aggregation period in the hierarchy, with attachment
	 * @param code	the code name of the aggregation period for which the lower level aggregation period needs to be returned
	 * @return the lower level aggregation period, or null if there is no aggregation period matching the code name or if the one matching the code name is a root
	 */
	public AggregationPeriodAndAttachment<T> getLowerLevelAggregationPeriodWithAttachment(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		return node == null ? null : 
			(node.lowerLevelNode == null ? null : node.lowerLevelNode.aggregationPeriodAndAttachment);
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy
	 * @param code	the code name of the aggregation period for which the upper level aggregation periods need to be returned
	 * @return the upper level aggregation periods, it will not be null but may be empty.
	 */
	public Set<AggregationPeriod> getUpperLevelAggregationPeriods(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		Set<AggregationPeriod> result = node == null ? null : 
			(node.upperLevels == null ? null : node.upperLevels.keySet());
		return result == null ? Collections.emptySortedSet() : result;
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy, with attachments
	 * @param code	the code name of the aggregation period for which the upper level aggregation periods need to be returned
	 * @return the sorted upper level aggregation periods with attachments, it will not be null but may be empty.
	 */
	public Collection<AggregationPeriodAndAttachment<T>> getUpperLevelAggregationPeriodsWithAttachments(String code){
		AggregationPeriodNode<T> node = codeMapping.get(code);
		Collection<AggregationPeriodAndAttachment<T>> result = node == null ? null : 
			(node.upperLevels == null ? null : node.upperLevels.values());
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
	 * Get the lower level aggregation period of an aggregation period in the hierarchy, with attachments
	 * @param aggregationPeriod	the aggregation period for which the lower level aggregation period needs to be returned
	 * @return	the lower level aggregation period with attachment, or null if the specified aggregation period is a root
	 */
	public AggregationPeriodAndAttachment<T> getLowerLevelAggregationPeriodWithAttachment(AggregationPeriod aggregationPeriod){
		return getLowerLevelAggregationPeriodWithAttachment(aggregationPeriod.getCodeName());
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy
	 * @param aggregationPeriod	the aggregation period for which the upper level aggregation periods need to be returned
	 * @return	the upper level aggregation periods, it will not be null but may be empty.
	 */
	public Set<AggregationPeriod> getUpperLevelAggregationPeriods(AggregationPeriod aggregationPeriod){
		return getUpperLevelAggregationPeriods(aggregationPeriod.getCodeName());
	}
	
	/**
	 * Get the upper level aggregation periods of an aggregation period in the hierarchy, with attachments
	 * @param aggregationPeriod	the aggregation period for which the upper level aggregation periods need to be returned
	 * @return	the sorted upper level aggregation periods with attachments, it will not be null but may be empty.
	 */
	public Collection<AggregationPeriodAndAttachment<T>> getUpperLevelAggregationPeriodsWithAttachments(AggregationPeriod aggregationPeriod){
		return getUpperLevelAggregationPeriodsWithAttachments(aggregationPeriod.getCodeName());
	}
	
	
}
