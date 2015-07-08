package net.sf.jabb.transprogtracker.mem;

import static org.junit.Assert.*;
import net.sf.jabb.taskqueues.mem.TransactionalProgressTrackerTest;
import net.sf.jabb.transprogtracker.TransactionalProgressTracker;

import org.junit.Test;

public class InMemTransactionalProgressTrackerTest extends TransactionalProgressTrackerTest{

	@Override
	protected TransactionalProgressTracker createTracker() {
		TransactionalProgressTracker tracker = new InMemTransactionalProgressTracker();
		return tracker;
	}

}
