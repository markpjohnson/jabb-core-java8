package net.sf.jabb.txprogress.mem;

import net.sf.jabb.txprogress.TransactionalProgress;
import net.sf.jabb.txprogress.TransactionalProgressTest;
import net.sf.jabb.txprogress.mem.InMemTransactionalProgress;

public class InMemTransactionalProgressTest extends TransactionalProgressTest{

	@Override
	protected TransactionalProgress createTracker() {
		TransactionalProgress tracker = new InMemTransactionalProgress();
		return tracker;
	}

}
