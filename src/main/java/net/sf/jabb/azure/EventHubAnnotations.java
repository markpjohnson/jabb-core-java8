package net.sf.jabb.azure;

import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Holder of Event Hub specific annotations.
 * It assumes that both x-opt-sequence-number and x-opt-offset are Long numbers.
 * @author James Hu
 *
 */
public class EventHubAnnotations{
	private long sequenceNumber;
	private long offset;
	private Instant enqueuedTime;
	
	/**
	 * Constructor
	 * @param json  such as: 
	 * 		<pre>
	 * 		{ "x-opt-sequence-number" : { "long" : 19650 }, "x-opt-offset" : "4302249320", "x-opt-enqueued-time" : { "timestamp" : 1438740137186 } }
	 * 		</pre>
	 */
	public EventHubAnnotations(String json){
		Validate.notBlank(json, "Event Hub x-opt-* meta data cannot be blank: " + json);
		
		String strSequenceNumber = findValue(json, "x-opt-sequence-number", "long");
		String strOffset = findValue(json, "x-opt-offset");
		String strEnqueuedTime = findValue(json, "x-opt-enqueued-time", "timestamp");
		
		Validate.isTrue(StringUtils.isNotBlank(strSequenceNumber) && StringUtils.isNotBlank(strOffset) && StringUtils.isNotBlank(strEnqueuedTime),
				"Event Hub x-opt-* meta data must cantain valid information: " + json);
		
		sequenceNumber = Long.parseLong(strSequenceNumber);
		offset = Long.parseLong(strOffset);
		enqueuedTime = Instant.ofEpochMilli(Long.parseLong(strEnqueuedTime));
	}
	
	private String findValue(String json, String label){
		return findValue(json, 0, label);
	}
	
	private String findValue(String json, int startPos, String label){
		int i = json.indexOf(label, startPos);
		if (i >= 0){
			i = json.indexOf(':', i + label.length());
			if (i >= 0){
				int start = i + 1;
				i = json.indexOf(',', start);
				if (i < 0){
					i = json.indexOf('}', start);
				}
				if (i >= 0){
					return trimValue(json.substring(start, i - 1));
				}
			}
		}
		return null;
	}
	
	private String findValue(String json, String label, String sublabel){
		int i = json.indexOf(label);
		if (i >= 0){
			i = json.indexOf(':', i + label.length());
			if (i >= 0){
				return findValue(json, i + 1, sublabel);
			}
		}
		return null;
	}
	
	private String trimValue(String value){
		return StringUtils.removeEnd(StringUtils.removeStart(value.trim(), "\""), "\"");
	}
	
	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public long getOffset() {
		return offset;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}
	public Instant getEnqueuedTime() {
		return enqueuedTime;
	}
}