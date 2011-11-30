package eu.stratosphere.pact.iterative.nephele.tasks.core;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.tasks.util.PactRecordEvent;


public class IterationTail extends AbstractIterativeTask {

	@Override
	public void invoke() throws Exception {
		//Read from input and forward backwards as events over the second inputgate (connects
		//to the iteration start). Events like close are forwarded too (need to think about this).
		InputGate<?> gate = getEnvironment().getInputGate(1);
		MutableObjectIterator<PactRecord> input = inputs[0];
		
		while(true) {
			try {
				PactRecord rec = new PactRecord();
				boolean success = input.next(rec);
				if(success) {
					gate.publishEvent(new PactRecordEvent(rec));
				}
				//Iterator is exhausted, when channel is closed = FINISHING
				if(!success) {
					break;
				}
			} catch (StateChangeException ex) {
				//Can records be lost here which are not yet read??
				if(stateListeners[0].getState() == ChannelState.CLOSED) {
					gate.publishEvent(new ChannelStateEvent(ChannelState.CLOSED));
				}
			}
		}
		
		//Read input from second gate so that nephele is satisfied
		inputs[1].next(new PactRecord());
		
		output.close();
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
	}


}