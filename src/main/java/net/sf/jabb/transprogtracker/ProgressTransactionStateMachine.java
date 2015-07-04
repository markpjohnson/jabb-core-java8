/**
 * 
 */
package net.sf.jabb.transprogtracker;

import static net.sf.jabb.transprogtracker.ProgressTransactionState.ABORTED;
import static net.sf.jabb.transprogtracker.ProgressTransactionState.FINISHED;
import static net.sf.jabb.transprogtracker.ProgressTransactionState.IN_PROGRESS;
import static net.sf.jabb.transprogtracker.ProgressTransactionState.TIMED_OUT;
import net.sf.jabb.util.state.StateMachineDefinition;
import net.sf.jabb.util.state.StateMachineWrapper;

/**
 * A simple state machine for ProcessingTransaction.
 * @author James Hu
 *
 */
public class ProgressTransactionStateMachine extends StateMachineWrapper<ProgressTransactionState, Integer>{
	private static final long serialVersionUID = -1307079273650491590L;

	//static public final Integer START = 4;
	static public final Integer ABORT = 5;
	static public final Integer FINISH = 6;
	static public final Integer TIME_OUT = 7;
	static public final Integer RETRY = 8;
	
	public ProgressTransactionStateMachine(){
		super();
	}
	
	public ProgressTransactionStateMachine(ProgressTransactionState initialState){
		this();
		setState(initialState);
	}
	
	@Override
	protected void define(StateMachineDefinition<ProgressTransactionState, Integer> definition) {
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
