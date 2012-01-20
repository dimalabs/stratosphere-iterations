package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public abstract class AbstractDualIterativeTask extends AbstractIterativeTask {
	protected IterationIterator iterationIterB;
	
	@Override
	public void invoke() throws Exception {
		MutableObjectIterator<PactRecord> inputA = inputs[0];
		MutableObjectIterator<PactRecord> inputB = inputs[1];
		ChannelStateTracker stateListenerA = stateListeners[0];
		ChannelStateTracker stateListenerB = stateListeners[1];
		
		boolean firstRound = true;
		
		IterationIterator iterationIterA = new IterationIterator(inputA, stateListenerA);
		iterationIterB = new IterationIterator(inputB, stateListenerB);
		
		while(!checkTermination(iterationIterA, iterationIterB)) {
			//Send iterative open state to output gates
			publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));
			
			if(firstRound) {
				invokeStart();
				firstRound = false;
			}
			
			//Call iteration stub function with the data for this iteration
			invokeIter(iterationIterA);
			
			if(stateListenerA.getState() == ChannelState.CLOSED &&
					stateListenerB.getState() == ChannelState.CLOSED) {
				publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
			} else {
				throw new RuntimeException("Illegal state after iteration call");
			}
		}
		
		cleanup();
		
		output.close();
	}
	
	public boolean checkTermination(IterationIterator iterA, IterationIterator iterB) throws IOException {
		boolean terminatedA = iterA.checkTermination();
		boolean terminatedB = iterB.checkTermination();
		if(terminatedA && terminatedB) {
			return true;
		} else if(!terminatedA && !terminatedB) {
			return false;
		} else {
			throw new RuntimeException("Both inputs have different channel states" + terminatedA + "::" + terminatedB);
		}
	}
}
