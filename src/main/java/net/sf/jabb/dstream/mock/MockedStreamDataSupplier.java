/**
 * 
 */
package net.sf.jabb.dstream.mock;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.SimpleReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

/**
 * Mocked StreamDataSupplier providing generated strings as events.
 * Events are generated according to the <code>eventsPerSecond</code> argument passed to the constructor.
 * The position of a mocked event is the epoch milliseconds of the event.
 * 
 * <p>
 * Every event is a JSON string with the following fields:
 * <ul>
 * 	<li>timeZone - always "UTC"</li>
 * 	<li>timestamp - Epoch milliseconds of the time this event was generated/enqueued</li>
 * 	<li>timestampString - same as timestamp, but of string type</li>
 * 	<li>date - Formatted string representation of the timestamp in UTC time zone</li>
 * 	<li>s1, s5, s10, s30 - second, second/5*5, second/10*10, second/30*30 </li>
 * 	<li>m1, m5, m10, m30 - minute, minute/5*5, minute/10*10, minute/30*30 </li>
 * 	<li>h1, h4, h6, h8, h12 - hour, hour/4*4, hour/6*6, hour/8*8, hour/12*12 </li>
 * 	<li></li>
 * </ul>
 * </p>
 * @author James Hu
 *
 */
public class MockedStreamDataSupplier implements StreamDataSupplier<String> {
	private static final Logger logger = LoggerFactory.getLogger(MockedStreamDataSupplier.class);
	
	private static DateTimeFormatter utcIsoDateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneId.of("UTC"));
	
	protected int intervalMillis;
	protected Instant firstEventTime;	// inclusive
	protected Instant lastEventTime;	// inclusive

	/**
	 * Constructor
	 * @param eventsPerSecond		number of events per second
	 * @param streamStartTime		the time that the stream starts to have new events, exclusive
	 * @param streamEndTime			the time that the stream stops to have new events, inclusive , can be null
	 */
	public MockedStreamDataSupplier(int eventsPerSecond, Instant streamStartTime, Instant streamEndTime){
		Validate.isTrue(eventsPerSecond >= 1 && eventsPerSecond <= 1000, "number of events per second must be between 1 and 1000");
		Validate.isTrue((1000 % eventsPerSecond) == 0, "1000 must be dividable by number of events per second");
			Validate.notNull(streamStartTime);
		
		this.intervalMillis = 1000 / eventsPerSecond;
		long et = (streamStartTime.toEpochMilli() / intervalMillis + 1)* intervalMillis;
		this.firstEventTime = Instant.ofEpochMilli(et);
		if (streamEndTime != null){
			et = streamEndTime.toEpochMilli() / intervalMillis * intervalMillis;
			this.lastEventTime = Instant.ofEpochMilli(et);
		}
	}
	
	protected String eventAt(Instant i){
		LocalDateTime utc = LocalDateTime.ofInstant(i, ZoneId.of("UTC"));
		StringBuilder sb = new StringBuilder();
		sb.append("{\"timeZone\": \"UTC\"");
		sb.append(", \"timestamp\": ").append(i.toEpochMilli());
		sb.append(", \"timestampString\": \"").append(i.toString()).append("\"");
		sb.append(", \"s1\": " ).append(utc.getSecond());
		sb.append(", \"s5\": " ).append(utc.getSecond() / 5 * 5);
		sb.append(", \"s10\": " ).append(utc.getSecond() / 10 * 10);
		sb.append(", \"s30\": " ).append(utc.getSecond() / 30 * 30);
		sb.append(", \"m1\": " ).append(utc.getMinute());
		sb.append(", \"m5\": " ).append(utc.getMinute() / 5 * 5);
		sb.append(", \"m10\": " ).append(utc.getMinute() / 10 * 10);
		sb.append(", \"m30\": " ).append(utc.getMinute() / 30 * 30);
		sb.append(", \"h1\": " ).append(utc.getHour());
		sb.append(", \"h4\": " ).append(utc.getHour() / 4 * 4);
		sb.append(", \"h6\": " ).append(utc.getHour() / 6 * 6);
		sb.append(", \"h8\": " ).append(utc.getHour() / 8 * 8);
		sb.append(", \"h12\": " ).append(utc.getHour() / 12 * 12);
		sb.append(", \"date\": \"" ).append(utc.format(utcIsoDateTimeFormatter)).append("\"");
		sb.append("}");
		return sb.toString();
	}

	@Override
	public String firstPosition() {
		return String.valueOf(-1);
	}

	@Override
	public String firstPosition(Instant enqueuedAfter, Duration waitForArrival) throws InterruptedException, DataStreamInfrastructureException {
		Instant eventTime = null;
		if (enqueuedAfter.isBefore(firstEventTime)){
			eventTime = firstEventTime;
		}else if (lastEventTime != null && lastEventTime.isBefore(enqueuedAfter)){
			Thread.sleep(waitForArrival.toMillis());
			return null;
		}else{
			long enqueuedAfterMillis = enqueuedAfter.toEpochMilli();
			long eventTimeMillis = (enqueuedAfterMillis / intervalMillis + 1) * intervalMillis;
			eventTime = Instant.ofEpochMilli(eventTimeMillis);
		}
		return String.valueOf(eventTime.toEpochMilli());
	}

	@Override
	public String lastPosition() throws DataStreamInfrastructureException {
		try {
			String firstPositionAfterNow = firstPosition(Instant.now(), Duration.ofMillis(1));
			if (firstPositionAfterNow == null){
				return String.valueOf(lastEventTime.toEpochMilli());
			}else{
				return String.valueOf(Long.valueOf(firstPositionAfterNow) - intervalMillis);
			}
		} catch (InterruptedException e) {
			new DataStreamInfrastructureException(e);
		}
		return null;
	}

	@Override
	public Instant enqueuedTime(String position) throws DataStreamInfrastructureException {
		return Instant.ofEpochMilli(Long.parseLong(position));
	}

	@Override
	public String nextStartPosition(String previousEndPosition) {
		return previousEndPosition;
	}

	@Override
	public boolean isInRange(String position, String endPosition) {
		Validate.isTrue(position != null, "position cannot be null");
		if (endPosition == null){
			return true;
		}else{
			return Long.parseLong(position) <= Long.parseLong(endPosition);
		}
	}

	@Override
	public boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime) {
		Validate.isTrue(enqueuedTime != null, "enqueuedTime cannot be null");
		if (endEnqueuedTime == null){
			return true;
		}else{
			return !enqueuedTime.isAfter(endEnqueuedTime);
		}
	}

	@Override
	public ReceiveStatus fetch(List<? super String> list, String startPosition, String endPosition, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		Instant timeout = Instant.now().plus(timeoutDuration);
		Long endPositionLong = endPosition == null ? null : Long.parseLong(endPosition);
		long position = Long.parseLong(startPosition);
		if (position < firstEventTime.toEpochMilli()){
			position = firstEventTime.toEpochMilli();
		}else{
			position = (position / intervalMillis + 1) * intervalMillis;
		}
		int i = 0;
		Long lastPosition = null;
		while((endPositionLong == null || position <= endPositionLong) && i ++ <= maxItems && Instant.now().isBefore(timeout)){
			if (position < System.currentTimeMillis()){
				lastPosition = position;
				list.add(eventAt(Instant.ofEpochMilli(position)));
			}
			position += intervalMillis;
		}
		
		if (logger.isDebugEnabled()){
			logger.debug("Fetched for ({}-{}],{},{}: ? - {}", startPosition, endPosition, maxItems, timeoutDuration, lastPosition);
		}
		if (lastPosition != null){
			return new SimpleReceiveStatus(String.valueOf(lastPosition), Instant.ofEpochMilli(lastPosition), endPositionLong != null && position > endPositionLong);
		}else{
			return new SimpleReceiveStatus(null, null, endPositionLong != null && position > endPositionLong);
		}
	}

	@Override
	public ReceiveStatus fetch(List<? super String> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		return fetch(list, String.valueOf(startEnqueuedTime.toEpochMilli()), 
				endEnqueuedTime == null ? null : String.valueOf(endEnqueuedTime.toEpochMilli()), 
						maxItems, timeoutDuration);
	}

	@Override
	public ReceiveStatus fetch(List<? super String> list, String startPosition, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		return fetch(list, startPosition, 
				endEnqueuedTime == null ? null : String.valueOf(endEnqueuedTime.toEpochMilli()), 
						maxItems, timeoutDuration);
	}

	@Override
	public String startAsyncReceiving(Consumer<String> receiver, String startPosition) throws DataStreamInfrastructureException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String startAsyncReceiving(Consumer<String> receiver, Instant startEnqueuedTime) throws DataStreamInfrastructureException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void stopAsyncReceiving(String id) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReceiveStatus receive(Function<String, Long> receiver, String startPosition, String endPosition) throws DataStreamInfrastructureException {
		Long endPositionLong = endPosition == null ? null : Long.parseLong(endPosition);
		long position = Long.parseLong(startPosition);
		position = (position / intervalMillis + 1) * intervalMillis;
		Long lastPosition = null;
		while(endPositionLong == null || position <= endPositionLong){
			if (receiver.apply(null) <= 0){
				break;
			}
			if (position < System.currentTimeMillis()){
				boolean done = receiver.apply(eventAt(Instant.ofEpochMilli(position))) <= 0;
				lastPosition = position;
				if (done){
					break;
				}
				position += intervalMillis;
			}
		}
		
		if (logger.isDebugEnabled()){
			logger.debug("Received for ({}-{}]: ? - {}", startPosition, endPosition, lastPosition);
		}
		if (lastPosition != null){
			return new SimpleReceiveStatus(String.valueOf(lastPosition), Instant.ofEpochMilli(lastPosition), endPositionLong != null && position > endPositionLong);
		}else{
			return new SimpleReceiveStatus(null, null, endPositionLong != null && position > endPositionLong);
		}
	}

	@Override
	public ReceiveStatus receive(Function<String, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		return receive(receiver, String.valueOf(startEnqueuedTime.toEpochMilli()), String.valueOf(endEnqueuedTime.toEpochMilli()));
	}

	@Override
	public ReceiveStatus receive(Function<String, Long> receiver, String startPosition, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		return receive(receiver, startPosition, String.valueOf(endEnqueuedTime.toEpochMilli()));
	}

	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() throws Exception {
	}

}
