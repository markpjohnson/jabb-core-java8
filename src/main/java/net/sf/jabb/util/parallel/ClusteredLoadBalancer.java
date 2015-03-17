/**
 * 
 */
package net.sf.jabb.util.parallel;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import com.google.common.base.Throwables;

/**
 * Load balancer clustered using jgroups for dispatching work loads to networked processing nodes.
 * Changes made to one instance will be propagated to all the member instances in the cluster.
 * @author James Hu
 *
 */
public class ClusteredLoadBalancer<L, R, P extends Serializable> extends BasicLoadBalancer<L, R, P> {
	protected JChannel channel;
	
	
	/**
	 * Constructor
	 * @param channel		the jgroups channel
	 * @param clusterName	the cluster name for the jgroups channel
	 * @param buckets	number of buckets for distributing the load evenly, normally it should be at least 10 times bigger than the number of processors.
	 * @param activeProcessors  processors, there must not be duplicated elements
	 * @param hashFunction	the function to create hash code for the work load dispatching
	 * @param dispatcher	actual dispatcher
	 */
	public ClusteredLoadBalancer(JChannel channel, String clusterName, int buckets, Collection<P> activeProcessors, ToIntFunction<L> hashFunction, BiFunction<P, L, R> dispatcher){
		this(channel, clusterName, buckets, activeProcessors, null, null, hashFunction, dispatcher, null, null);
	}


	/**
	 * Constructor
	 * @param channel		the jgroups channel
	 * @param clusterName	the cluster name for the jgroups channel
	 * @param buckets	number of buckets for distributing the load evenly, normally it should be at least 10 times bigger than the number of processors.
	 * @param activeProcessors  processors, there must not be duplicated elements
	 * @param backupProcessors	backup processors which will not be used at the beginning, there must not be duplicated elements. It can be null.
	 * @param fallback	the fall back processor that handles all the failed-to-dispatch work loads. It can be null.
	 * @param hashFunction	the function to create hash code for the work load dispatching
	 * @param dispatcher	actual dispatcher
	 * @param statistics	statistics collector. It can be null.
	 * @param monitor		monitor that receives copies of all of the work load. It can be null.
	 */
	public ClusteredLoadBalancer(JChannel channel, String clusterName, int buckets, Collection<P> activeProcessors, Collection<P> backupProcessors, 
			Consumer<L> fallback, ToIntFunction<L> hashFunction, BiFunction<P, L, R> dispatcher, Consumer<DispatchingStatistics<L, P>> statistics, Consumer<L> monitor){
		super(buckets, activeProcessors, backupProcessors, fallback, hashFunction, dispatcher, statistics, monitor);
		this.channel = channel;
		channel.setReceiver(new Receiver());
		try {
			channel.connect(clusterName);
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	public View getClusterView(){
		return channel.getView();
	}
	
	@Override
	public void finalize(){
		channel.close();
	}
	
	synchronized protected void setState(StateData<P> stateData){
		if (stateData.getProcessorMap() != null){
			this.processorMap = stateData.getProcessorMap();
		}
		if (stateData.getActiveProcessors() != null){
			this.activeProcessors = stateData.getActiveProcessors();
		}
		if (stateData.getBackupProcessors() != null){
			this.backupProcessors = stateData.getBackupProcessors();
		}
	}
	
	synchronized protected StateData<P> getState(boolean withProcessorMap, boolean withActiveProcessors, boolean withBackupProcessors){
		return new StateData<P>(withProcessorMap ? null : processorMap, 
				withActiveProcessors ? null : activeProcessors, 
				withBackupProcessors ? null : backupProcessors);
	}
	
	protected StateData<P> getState(){
		return getState(true, true, true);
	}
	
	/**
	 * Propagate state to other members in the cluster
	 * @param withProcessorMap		whether to propagate processorMap
	 * @param withActiveProcessors	whether to propagate activeProcessors
	 * @param withBackupProcessors	whether to propagate backupProcessors
	 */
	protected void propagateState(boolean withProcessorMap, boolean withActiveProcessors, boolean withBackupProcessors){
		try {
			channel.send(null, getState(withProcessorMap, withActiveProcessors, withBackupProcessors));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Propagate full state to other members in the cluster
	 */
	protected void propagateState(){
		propagateState(true, true, true);
	}

	
	protected class Receiver extends ReceiverAdapter{
		@Override
		public void receive(Message msg){
			try {
				Object payload = Util.objectFromByteBuffer(msg.getBuffer());
				if (payload instanceof StateData){
					@SuppressWarnings("unchecked")
					StateData<P> stateData = (StateData<P>) payload;
					ClusteredLoadBalancer.this.setState(stateData);
				}
			} catch (Exception e) {
				// ignore
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void getState(OutputStream output) throws Exception {
			synchronized(ClusteredLoadBalancer.this){
				Util.objectToStream(ClusteredLoadBalancer.this.getState(), new DataOutputStream(output));
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void setState(InputStream input) throws Exception {
			synchronized(ClusteredLoadBalancer.this){
				ClusteredLoadBalancer.this.setState((StateData<P>) Util.objectFromStream(new DataInputStream(input)));
			}			
		}
		
	}
	
	/**
	 * Data needed for state replication
	 * @author James Hu
	 *
	 * @param <P>	type of the processor
	 */
	public static class StateData<P extends Serializable> implements Serializable{
		private static final long serialVersionUID = 7186584292417807338L;
		protected Object[] processorMap;
		protected List<P> activeProcessors;
		protected List<P> backupProcessors;
		
		public StateData(){
			
		}
		
		public StateData(Object[] processorMap, List<P> activeProcessors, List<P> backupProcessors){
			this();
			this.processorMap = processorMap;
			this.activeProcessors = activeProcessors;
			this.backupProcessors = backupProcessors;
		}

		public Object[] getProcessorMap() {
			return processorMap;
		}

		public void setProcessorMap(Object[] processorMap) {
			this.processorMap = processorMap;
		}

		public List<P> getActiveProcessors() {
			return activeProcessors;
		}

		public void setActiveProcessors(List<P> activeProcessors) {
			this.activeProcessors = activeProcessors;
		}

		public List<P> getBackupProcessors() {
			return backupProcessors;
		}

		public void setBackupProcessors(List<P> backupProcessors) {
			this.backupProcessors = backupProcessors;
		}
		
	}
	
	/**
	 * Increase the load of a processor
	 * @param processor	the processor
	 * @param percentageDelta	the percentage of load in total that will be increased
	 */
	@Override
	synchronized public void increaseLoad(P processor, float percentageDelta){
		super.increaseLoad(processor, percentageDelta);
		propagateState(true, false, false);
	}
	
	/**
	 * Decrease the load of a processor
	 * @param processor	the processor
	 * @param percentageDelta	the percentage of load in total that will be decreased
	 */
	@Override
	synchronized public void decreaseLoad(P processor, float percentageDelta){
		super.decreaseLoad(processor, percentageDelta);
		propagateState(true, false, false);
	}
	
	/**
	 * Add a processor. Work load will be dispatched to the newly added processor immediately.
	 * @param processor	the processor
	 */
	@Override
	synchronized public void add(P processor){
		super.add(processor);
		propagateState(true, true, false);
	}
	
	/**
	 * Remove a processor. The dispatcher will stop dispatching work load to the newly removed processor immediately.
	 * @param processor	the processor
	 */
	@Override
	synchronized public void remove(P processor){
		super.remove(processor);
		propagateState(true, true, false);
	}

	/**
	 * Add a backup processor 
	 * @param processor	the backup processor
	 */
	@Override
	synchronized public void addBackup(P processor){
		super.addBackup(processor);
		propagateState(false, false, true);
	}
	
	/**
	 * Remove a backup processor
	 * @param processor	the backup processor
	 */
	@Override
	synchronized public void removeBackup(P processor){
		super.removeBackup(processor);
		propagateState(false, false, true);
	}

	/**
	 * Promote a backup processor to active 
	 * @param processor		the backup processor
	 */
	@Override
	synchronized public void promote(P processor){
		super.promote(processor);
		propagateState();
	}
	
	/**
	 * Demote an active processor to backup 
	 * @param processor	the active processor
	 */
	@Override
	synchronized public void demote(P processor){
		super.demote(processor);
		propagateState();
	}
	
	/**
	 * Replace an active processor with another new processor
	 * @param processor		the processor to be replaced
	 * @param newProcessor	the new processor
	 */
	@Override
	synchronized public void replace(P processor, P newProcessor){
		super.replace(processor, newProcessor);
		propagateState(true, true, false);
	}
	
	/**
	 * Replace a backup processor with another new processor
	 * @param processor		the backup processor to be replaced
	 * @param newProcessor	the new processor
	 */
	@Override
	synchronized public void replaceBackup(P processor, P newProcessor){
		super.replaceBackup(processor, newProcessor);
		propagateState(false, false, true);
	}
	

}
