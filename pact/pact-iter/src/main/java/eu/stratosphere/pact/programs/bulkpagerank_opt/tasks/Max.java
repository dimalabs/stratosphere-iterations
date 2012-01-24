package eu.stratosphere.pact.programs.bulkpagerank_opt.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;

public class Max extends AbstractIterativeTask {
	PactRecord rec = new PactRecord();
	PactDouble number = new PactDouble();
	
	@Override
	public void invokeStart() throws Exception {
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		double max = Double.NEGATIVE_INFINITY;
		
		while(iterationIter.next(rec)) {
			double value = rec.getField(0, number).getValue();
			if(value > max) {
				max = value;
			}
		}
		
		number.setValue(max);
		rec.setField(0, number);
		output.collect(rec);
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
