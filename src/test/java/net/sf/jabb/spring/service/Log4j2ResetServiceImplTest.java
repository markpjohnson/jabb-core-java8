/**
 * 
 */
package net.sf.jabb.spring.service;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author James Hu (Zhengmao Hu)
 *
 */
public class Log4j2ResetServiceImplTest {

	//@Test
	public void test() {
		Log4j2ResetServiceImpl resetSvc = new Log4j2ResetServiceImpl();
		resetSvc.resetLogger();
	}

}
