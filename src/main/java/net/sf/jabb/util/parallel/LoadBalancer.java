package net.sf.jabb.util.parallel;

public interface LoadBalancer<L, R, P> extends LoadDispatcher<L, R>{

	/**
	 * Increase the load of a processor
	 * @param processor	the processor
	 * @param percentageDelta	the percentage of load in total that will be increased
	 */
	void increaseLoad(P processor, float percentageDelta);

	/**
	 * Decrease the load of a processor
	 * @param processor	the processor
	 * @param percentageDelta	the percentage of load in total that will be decreased
	 */
	void decreaseLoad(P processor, float percentageDelta);

	/**
	 * Add a processor. Work load will be dispatched to the newly added processor immediately.
	 * @param processor	the processor
	 */
	void add(P processor);

	/**
	 * Remove a processor. The dispatcher will stop dispatching work load to the newly removed processor immediately.
	 * @param processor	the processor
	 */
	void remove(P processor);

	/**
	 * Add a backup processor 
	 * @param processor	the backup processor
	 */
	void addBackup(P processor);

	/**
	 * Remove a backup processor
	 * @param processor	the backup processor
	 */
	void removeBackup(P processor);

	/**
	 * Promote a backup processor to active 
	 * @param processor		the backup processor
	 */
	void promote(P processor);

	/**
	 * Demote an active processor to backup 
	 * @param processor	the active processor
	 */
	void demote(P processor);

	/**
	 * Replace an active processor with another new processor
	 * @param processor		the processor to be replaced
	 * @param newProcessor	the new processor
	 */
	void replace(P processor, P newProcessor);

	/**
	 * Replace a backup processor with another new processor
	 * @param processor		the backup processor to be replaced
	 * @param newProcessor	the new processor
	 */
	void replaceBackup(P processor, P newProcessor);

}