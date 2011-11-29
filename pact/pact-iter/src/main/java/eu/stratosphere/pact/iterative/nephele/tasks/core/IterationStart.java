package eu.stratosphere.pact.iterative.nephele.tasks.core;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterChannelStateEvent.ChannelState;

public class IterationStart extends AbstractMinimalTask {

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		//Send iterative open event
		getEnvironment().getOutputGate(0).publishEvent(new IterChannelStateEvent(ChannelState.OPEN));
		getEnvironment().getOutputGate(0).flush();
		
		Thread.sleep(5000);
		
		//Read from input and forward to output
		MutableObjectIterator<PactRecord> input = inputs[0];
		PactRecord rec = new PactRecord();
		while(input.next(rec)) {
			output.collect(rec);
		}
		
		//Send iterative close event
		getEnvironment().getOutputGate(0).publishEvent(new IterChannelStateEvent(ChannelState.CLOSED));
		getEnvironment().getOutputGate(0).flush();
		
		//Close output
		output.close();
	}

}
