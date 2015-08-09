/**
 * 
 */
package net.sf.jabb.seqtx;

import static net.sf.jabb.seqtx.SequentialTransactionState.ABORTED;
import static net.sf.jabb.seqtx.SequentialTransactionState.FINISHED;
import static net.sf.jabb.seqtx.SequentialTransactionState.IN_PROGRESS;
import static net.sf.jabb.seqtx.SequentialTransactionState.TIMED_OUT;
import net.sf.jabb.util.state.StateMachineDefinition;
import net.sf.jabb.util.state.StateMachineWrapper;

/**
 * A simple state machine for ProcessingTransaction.
 * @author James Hu
 *
 */
public class SequentialTransactionStateMachine extends StateMachineWrapper<SequentialTransactionState, Integer>{
	private static final long serialVersionUID = -1307079273650491590L;

	//static public final Integer START = 4;
	static public final Integer ABORT = 5;
	static public final Integer FINISH = 6;
	static public final Integer TIME_OUT = 7;
	static public final Integer RETRY = 8;
	
	public SequentialTransactionStateMachine(){
		super();
	}
	
	public SequentialTransactionStateMachine(SequentialTransactionState initialState){
		this();
		setState(initialState);
	}
	
	@Override
	protected void define(StateMachineDefinition<SequentialTransactionState, Integer> definition) {
		definition
		.addState(IN_PROGRESS)
		.addState(ABORTED)
		.addState(FINISHED)
		.addState(TIMED_OUT)
	
		.addTransition(ABORT, IN_PROGRESS, ABORTED)
		.addTransition(FINISH, IN_PROGRESS, FINISHED)
		.addTransition(TIME_OUT, IN_PROGRESS, TIMED_OUT)
	
		.addTransition(RETRY, ABORTED, IN_PROGRESS)
		.addTransition(RETRY, TIMED_OUT, IN_PROGRESS);
	}

	public boolean abort(){
		return transit(ABORT);
	}
	
	public boolean finish(){
		return transit(FINISH);
	}
	
	public boolean timeout(){
		return transit(TIME_OUT);
	}
	
	public boolean retry(){
		return transit(RETRY);
	}
	
	public boolean isInprogress(){
		return getState().equals(IN_PROGRESS);
	}
	
	public boolean isAborted(){
		return getState().equals(ABORTED);
	}
	
	public boolean isFinished(){
		return getState().equals(FINISHED);
	}
	
	public boolean isTimedOut(){
		return getState().equals(TIMED_OUT);
	}

	
}
