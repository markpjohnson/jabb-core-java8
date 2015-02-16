/**
 * 
 */
package net.sf.jabb.util.stat;

/**
 * Multi-thread safe statistics holder for high precision use cases.
 * @author James Hu
 *
 */
public class AdvancedNumberStatistics {
	private BigIntegerAdder count;
	private BigIntegerAdder sum;
}
