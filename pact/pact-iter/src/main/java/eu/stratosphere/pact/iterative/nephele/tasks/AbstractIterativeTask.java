package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public abstract class AbstractIterativeTask extends AbstractMinimalTask {
	
	protected ChannelStateTracker[] stateListeners;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initInternal() {
		Environment env = getEnvironment();
		int numInputs = getNumberOfInputs();
		
		stateListeners = new ChannelStateTracker[numInputs];
		for (int i = 0; i < numInputs; i++)
		{
			InputGate<PactRecord> inputGate = (InputGate<PactRecord>) env.getInputGate(i);
			stateListeners[i] = new ChannelStateTracker(inputGate.getNumberOfInputChannels());
			inputGate.subscribeToEvent(stateListeners[i], ChannelStateEvent.class);
		}
	}

	@Override
	public void invoke() throws Exception {
		MutableObjectIterator<PactRecord> input = inputs[0];
		ChannelStateTracker stateListener = stateListeners[0];
		
		PactRecord rec = new PactRecord();
		
		while(true) {
			try {
				boolean success = input.next(rec);				
				if(success && stateListener.getState() == ChannelState.OPEN) {
					IterationIterator iterationIter = new IterationIterator(rec, input, stateListener);
					//Call iteration stub function
					invokeIter(iterationIter);
					
					//When exiting invoke iter, channel state should be closed
					if(stateListener.getState() == ChannelState.CLOSED) {
						publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
					} else {
						throw new RuntimeException("Illegal state after iteration call");
					}
				} else if(success) {
					//A record was received, but the iteration channel state is not open
					// => Error
					throw new RuntimeException("Record received during closed channel");
				} else {
					//If it returned, but there is no state change the iterator is exhausted 
					// => Finishing
					break;
				}
			} catch (StateChangeException ex) {
				if(stateListener.isChanged()) {
					ChannelState state = stateListener.getState();
					if(state == ChannelState.OPEN) {
						publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));
					} 
					else if(state == ChannelState.CLOSED) {
						publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
					}
				} 
			}
		}
				
		output.close();
	}

	public abstract void invokeIter(IterationIterator iterationIter) throws Exception;
	
	protected static void publishState(ChannelState state, OutputGate<?> gate) throws Exception {
		gate.flush(); //So that all records are hopefully send in a seperate envelope from the event
		//Now when the event arrives, all records will be processed before the state change.
		gate.publishEvent(new ChannelStateEvent(state));
		gate.flush();
	}	
}
