package eu.stratosphere.pact.programs.bulkpagerank_broad.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class InitialRankAssigner extends AbstractMinimalTask {
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		PactRecord rec = new PactRecord();
		PactDouble initialRank = new PactDouble(1d / 14052);
		
		while(inputs[0].next(rec)) {
			rec.setField(1, initialRank);
			output.collect(rec);
		}
	}

}
