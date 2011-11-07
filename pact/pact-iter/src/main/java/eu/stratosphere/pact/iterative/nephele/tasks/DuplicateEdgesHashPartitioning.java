package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.PartitionFunction;

public class DuplicateEdgesHashPartitioning extends AbstractMinimalTask {

	@Override
	public void invoke() throws Exception {
		//Fix output emitter to use simple hash partitioning
		OutputEmitter oe = 
				(OutputEmitter) output.getWriters().get(0).getOutputGate().getChannelSelector();
		oe.setPartitionFunction(new PartitionFunction() {
			
			@Override
			public int[] selectChannels(PactRecord data, int numChannels) {
				return new int[] { data.hashCode() % numChannels };
			}
		});
		
		PactRecord original = new PactRecord();
		PactRecord swapped = new PactRecord();
		
		MutableObjectIterator<PactRecord> input = inputs[0];
		
		while(input.next(original)) {
			PactInteger nodeId = original.getField(0, PactInteger.class);
			PactInteger neighbourId = original.getField(1, PactInteger.class);
			
			swapped.setField(0, neighbourId);
			swapped.setField(1, nodeId);
			
			output.collect(original);
			output.collect(swapped);
		}
		
		output.close();
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}
}
