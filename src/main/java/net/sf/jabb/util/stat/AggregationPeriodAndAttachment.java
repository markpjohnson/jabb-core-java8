/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;

/**
 * Combination of an AggregationPeriod and an attachment object.
 * @param <T> type of the attachment
 * @author James Hu
 *
 */
public class AggregationPeriodAndAttachment<T> implements Serializable{
	private static final long serialVersionUID = 6902504936712686015L;

	AggregationPeriod aggregationPeriod;
	T attachment;
	
	public AggregationPeriodAndAttachment(){
		
	}
	
	public AggregationPeriodAndAttachment(AggregationPeriod aggregationPeriod, T attachment){
		this.aggregationPeriod = aggregationPeriod;
		this.attachment = attachment;
	}
	
	/**
	 * @return the aggregationPeriod
	 */
	public AggregationPeriod getAggregationPeriod() {
		return aggregationPeriod;
	}
	/**
	 * @param aggregationPeriod the aggregationPeriod to set
	 */
	public void setAggregationPeriod(AggregationPeriod aggregationPeriod) {
		this.aggregationPeriod = aggregationPeriod;
	}
	/**
	 * @return the attachment
	 */
	public T getAttachment() {
		return attachment;
	}
	/**
	 * @param attachment the attachment to set
	 */
	public void setAttachment(T attachment) {
		this.attachment = attachment;
	}
}
