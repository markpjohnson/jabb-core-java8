/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class BigIntegerAdderTest {

	@Test
	public void testAssumptions() {
		assertEquals(Long.MAX_VALUE, 2L * (2L + Integer.MAX_VALUE) * Integer.MAX_VALUE + 1);
		assertTrue(BigIntegerAdder.THRESHOLD_POSITIVE > 10_000L * Integer.MAX_VALUE);
	}

}
