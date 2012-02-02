package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;

public class CountUpdates extends AbstractIterativeTask {
	PactRecord record = new PactRecord();
	PactRecord result = new PactRecord();
	PactLong count = new PactLong();
	
	@Override
	public void invokeStart() throws Exception {
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		int counter = 0;
		
		while(iterationIter.next(record)) {
			counter++;
		}
		
		count.setValue(counter);
		result.setField(0, count);
		output.collect(result);
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
