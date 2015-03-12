/**
 * 
 */
package net.sf.jabb.util.text;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class UuidEncodingTest {

	@Test
	public void test(){
		for (int i = 0; i < 100; i ++){
			doTest();
		}
	}
	
	protected void doTest() {
		UUID uuid = UUID.randomUUID();
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		
		Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
		String encoded = enc.encodeToString(bb.array());
		assertNotNull(encoded);
		assertEquals(22, encoded.length());
		//System.out.println(encoded);
		//System.out.println(encoded.length());
	}

}
