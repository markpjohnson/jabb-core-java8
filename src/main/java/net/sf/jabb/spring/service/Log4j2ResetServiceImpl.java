/**
 * 
 */
package net.sf.jabb.spring.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;

/**
 * Implementation of LoggerResetService as a spring service for Log4j2.
 * When the setBeanName(...) method is called by Spring context, Log4j2 context will be reset.
 * @author James Hu
 *
 */
public class Log4j2ResetServiceImpl implements LoggerResetService, BeanNameAware {
	static private final Logger logger = LoggerFactory.getLogger(Log4j2ResetServiceImpl.class);

	@Override
	public void resetLogger() {
		logger.debug("Resetting Log4j2...");
		try{
			final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
	        final Configuration config = ctx.getConfiguration();
	        Configurator.initialize(null, config.getConfigurationSource());
		} catch(Exception e) {
			logger.error("Failed to reset Log4j2", e);
		}	
	    logger.info("Log4j2 has been reset.");
	}

	@Override
	public void setBeanName(String name) {
		// setBeanName(...) method will called the earliest.
		resetLogger();
	}

}
