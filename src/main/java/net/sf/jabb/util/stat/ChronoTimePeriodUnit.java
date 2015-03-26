/**
 * 
 */
package net.sf.jabb.util.stat;

import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

/**
 * Unit of time periods. This class bridges between ChronoUnit, TimeUnit and Calendar.
 *  For units smaller than hour, they are represented as ChronoUnit and TimeUnit;
 *  For units equals to or larger than hour, they are represented as ChronoUnit and Calendar fields.
 * @author James Hu
 *
 */
public enum ChronoTimePeriodUnit {
    MILLISECONDS(ChronoUnit.MILLIS, TimeUnit.MILLISECONDS, Calendar.MILLISECOND, 1L, 'I', TimePeriodUnit.MILLISECONDS), 
    SECONDS(ChronoUnit.SECONDS, TimeUnit.SECONDS, Calendar.SECOND, 1000L, 'S', TimePeriodUnit.SECONDS), 
    MINUTES(ChronoUnit.MINUTES, TimeUnit.MINUTES, Calendar.MINUTE, 1000L * 60, 'N', TimePeriodUnit.MINUTES), 
	HOURS(ChronoUnit.HOURS, TimeUnit.HOURS, Calendar.HOUR_OF_DAY, 1000L * 3600, 'H', TimePeriodUnit.HOURS), 
	DAYS(ChronoUnit.DAYS, TimeUnit.DAYS, Calendar.DAY_OF_MONTH, 1000L * 3600 * 24, 'D', TimePeriodUnit.DAYS),  // 1 means 1st day of a month
	WEEKS(ChronoUnit.WEEKS, null, Calendar.DAY_OF_WEEK, 1000L * 3600 * 24 * 7, 'W', TimePeriodUnit.WEEKS),
	MONTHS(ChronoUnit.MONTHS, null, Calendar.MONTH, 2630000000L, 'M', TimePeriodUnit.MONTHS),  // 0 means January
	YEARS(ChronoUnit.YEARS, null, Calendar.YEAR, 31556900000L, 'Y', TimePeriodUnit.YEARS);

    private ChronoUnit chronoUnit;
    private TimeUnit timeUnit;
    private int calendarField;
    private long milliseconds;
    private char code;
    private TimePeriodUnit timePeriodUnit;

    ChronoTimePeriodUnit(ChronoUnit chronoUnit, TimeUnit timeUnit, int calendarField, long milliseconds, char code, TimePeriodUnit timePeriodUnit){
    	this.chronoUnit = chronoUnit;
    	this.timeUnit = timeUnit;
    	this.calendarField = calendarField;
    	this.milliseconds = milliseconds;
    	this.code = code;
    	this.timePeriodUnit = timePeriodUnit;
    }

    /**
     * Get the corresponding ChronoTimePeriodUnit from TimeUnit. If no matching ChronoTimePeriodUnit
     * can be found, IllegalArgumentException will be thrown.
     * @param timeUnit	the time unit, can be null
     * @return	the ChronoTimePeriodUnit, can be null if the input is null.
     */
    static public ChronoTimePeriodUnit of(TimeUnit timeUnit){
    	if (timeUnit == null){
    		return null;
    	}
    	switch(timeUnit){
	    	case MILLISECONDS:
	    		return MILLISECONDS;
	    	case SECONDS:
	    		return SECONDS;
	    	case MINUTES:
	    		return MINUTES;
	    	case HOURS:
	    		return HOURS;
	    	case DAYS:
	    		return DAYS;
	    	default:
	    		throw new IllegalArgumentException("Unsupported TimeUnit: " + timeUnit);
    	}
    }
    
    /**
     * Get the corresponding ChronoTimePeriodUnit from ChronoUnit. If no matching ChronoTimePeriodUnit
     * can be found, IllegalArgumentException will be thrown.
     * @param chronoUnit	the chrono unit, can be null
     * @return	the ChronoTimePeriodUnit, can be null if the input is null.
     */
    static public ChronoTimePeriodUnit of(ChronoUnit chronoUnit){
    	if (chronoUnit == null){
    		return null;
    	}
    	switch(chronoUnit){
	    	case MILLIS:
	    		return MILLISECONDS;
	    	case SECONDS:
	    		return SECONDS;
	    	case MINUTES:
	    		return MINUTES;
	    	case HOURS:
	    		return HOURS;
	    	case DAYS:
	    		return DAYS;
	    	case WEEKS:
	    		return WEEKS;
	    	case MONTHS:
	    		return MONTHS;
	    	case YEARS:
	    		return YEARS;
	    	default:
	    		throw new IllegalArgumentException("Unsupported ChronoUnit: " + chronoUnit);
    	}
    }
    
    /**
     * Get the corresponding ChronoTimePeriodUnit from TimePeriodUnit. If no matching ChronoTimePeriodUnit
     * can be found, IllegalArgumentException will be thrown.
     * @param timePeriodUnit  the TimePeriodUnit, can be null
     * @return	the ChronoTimePeriodUnit, can be null if the input is null.
     */
    static public ChronoTimePeriodUnit of (TimePeriodUnit timePeriodUnit){
    	if (timePeriodUnit == null){
    		return null;
    	}
    	switch(timePeriodUnit){
	    	case MILLISECONDS:
	    		return MILLISECONDS;
	    	case SECONDS:
	    		return SECONDS;
	    	case MINUTES:
	    		return MINUTES;
	    	case HOURS:
	    		return HOURS;
	    	case DAYS:
	    		return DAYS;
	    	case WEEKS:
	    		return WEEKS;
	    	case MONTHS:
	    		return MONTHS;
	    	case YEARS:
	    		return YEARS;
	    	default:
	    		throw new IllegalArgumentException("Unsupported TimePeriodUnit: " + timePeriodUnit);
	    	}
    }

    
    /**
     * Get the ChronoTimePeriodUnit that the one-character code represents
     * @param code	the one-character code
     * @return	the corresponding ChronoTimePeriodUnit
     */
    static public ChronoTimePeriodUnit from (char code){
    	switch(code){
	    	case 'I':
	    		return MILLISECONDS;
	    	case 'S':
	    		return SECONDS;
	    	case 'N':
	    		return MINUTES;
	    	case 'H':
	    		return HOURS;
	    	case 'D':
	    		return DAYS;
	    	case 'W':
	    		return WEEKS;
	    	case 'M':
	    		return MONTHS;
	    	case 'Y':
	    		return YEARS;
    		default:
	    		throw new IllegalArgumentException("Unknown: " + code);
    	}
    }
    
    /**
     * Get the one-character code for this unit
     * @return	the one-character code
     */
    public char toShortCode(){
    	return code;
    }
	
    /**
     * Get the corresponding ChronoUnit.
     * @return corresponding ChronoUnit
     */
    public ChronoUnit toChronoUnit(){
        return chronoUnit;
    }
    
    /**
     * Get the corresponding TimeUnit.
     * @return corresponding TimeUnit or null for WEEKS, MONTHS and YEARS
     */
    public TimeUnit toTimeUnit(){
        return timeUnit;
    }
    
    public int toCalendarField(){
        return calendarField;
    }

	/**
	 * Number of milliseconds this unit represents. However for MONTHS and YEARS the value
	 * returned are just average numbers.
	 * @return number of milliseconds
	 */
	public long toMilliseconds(){
		return milliseconds;
	}
	
	/**
	 * Get the corresponding TimePeriodUnit
	 * @return the corresponding TimePeriodUnit
	 */
    public TimePeriodUnit toTimePeriodUnit(){
    	return timePeriodUnit;
    }
    
	public boolean isDivisorOf(ChronoTimePeriodUnit that){
		if (this == that){
			return true;
		}
		switch(that){
			case YEARS:
				return this != WEEKS;
			case MONTHS:
				return this != WEEKS && this != MONTHS;
			default:
				return this.isShorterThan(that); // && (that.milliseconds % this.milliseconds) == 0;
		}
	}

	public boolean isShorterThan(ChronoTimePeriodUnit another) {
		return this.milliseconds < another.milliseconds;
	}

	public boolean isLongerThan(ChronoTimePeriodUnit another) {
		return this.milliseconds > another.milliseconds;
	}
	
	/**
	 * Parse a string to get the ChronoTimePeriodUnit it represents.
	 * @param unitString the string to be parsed, it should just be the name of an ChronoTimePeriodUnit enum, 
	 *  but can contain leading and trailing blank spaces, 
	 * 	can have mixed upper and lower cases, and can have the last letter 's' missing. 
	 * 	For example, 'hour', 'Hour', 'HOUR', 'hours', and 'hOUrs' are all valid.
	 * @return	the ChronoTimePeriodUnit represented by the input string
	 */
	static public ChronoTimePeriodUnit from(String unitString) {
		unitString = unitString.trim().toUpperCase();
		if(unitString.charAt(unitString.length() - 1) != 'S') {
			unitString = unitString + "S";
		}
		return ChronoTimePeriodUnit.valueOf(unitString);
	}

}
