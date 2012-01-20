package eu.stratosphere.pact.iterative.nephele.bulk;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

public class BulkIterationHead extends IterationHead {
	private PactRecord rec = new PactRecord();
	
	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter,
			OutputCollector innerOutput) throws Exception {
		while(iter.next(rec)) {
			innerOutput.collect(rec);
		}
	}

	@Override
	public void processUpdates(MutableObjectIterator<PactRecord> iter,
			OutputCollector innerOutput) throws Exception {
		processInput(iter, innerOutput);
	}
	
	@Override
	public void finish(MutableObjectIterator<PactRecord> iter,
			OutputCollector iterationOutput) throws Exception {
		while(iter.next(rec)) {
			iterationOutput.collect(rec);
		}
	}
}
