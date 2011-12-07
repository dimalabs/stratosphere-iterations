package eu.stratosphere.pact.iterative.nephele.tasks.core;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public class IterationStateSynchronizer extends AbstractIterativeTask {
	
	@Override
	protected void initTask() {
	}

	@Override
	public void invoke() throws Exception {
		MutableObjectIterator<PactRecord> input = inputs[0];
		ChannelStateTracker stateListener = stateListeners[0];
		
		PactRecord rec = new PactRecord();
		
		while(true) {
			try {
				boolean success = input.next(rec);				
				if(success) {
					throw new RuntimeException("Received record");
				} else {
					//If it returned, but there is no state change the iterator is exhausted 
					// => Finishing
					break;
				}
			} catch (StateChangeException ex) {
				if(stateListener.isChanged() && stateListener.getState() == ChannelState.CLOSED) {
					getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.CLOSED));
				} 
			}
		}
		
		//Satisfy nephele
		inputs[1].next(rec);
		
		output.close();
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
	}

	

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

}
