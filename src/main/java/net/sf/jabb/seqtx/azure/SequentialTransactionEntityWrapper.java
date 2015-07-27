/**
 * 
 */
package net.sf.jabb.seqtx.azure;

/**
 * @author James Hu
 *
 */
public class SequentialTransactionEntityWrapper {
	protected SequentialTransactionEntity entity;
	protected SequentialTransactionEntityWrapper previous;
	protected SequentialTransactionEntityWrapper next;
	
	public SequentialTransactionEntityWrapper(){
		
	}
	
	public SequentialTransactionEntityWrapper(SequentialTransactionEntity entity){
		this.entity = entity;
	}
	
	
	public SequentialTransactionEntity getEntity() {
		return entity;
	}
	public void setEntity(SequentialTransactionEntity entity) {
		this.entity = entity;
	}
	public SequentialTransactionEntityWrapper getPrevious() {
		return previous;
	}
	public void setPrevious(SequentialTransactionEntityWrapper previous) {
		this.previous = previous;
	}
	public SequentialTransactionEntityWrapper getNext() {
		return next;
	}
	public void setNext(SequentialTransactionEntityWrapper next) {
		this.next = next;
	}
}
