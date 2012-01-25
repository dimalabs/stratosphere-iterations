package eu.stratosphere.pact.programs.bulkpagerank_broad.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;

public class Forward extends AbstractIterativeTask {
	PactRecord rec = new PactRecord();
	
	@Override
	public void invokeStart() throws Exception {
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		while(iterationIter.next(rec)) {
			output.collect(rec);
		}
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
