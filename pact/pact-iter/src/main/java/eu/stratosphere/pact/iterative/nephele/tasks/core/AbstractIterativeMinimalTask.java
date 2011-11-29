package eu.stratosphere.pact.iterative.nephele.tasks.core;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateListener;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterChannelStateEvent.ChannelState;



public abstract class AbstractIterativeMinimalTask extends AbstractMinimalTask {
	
	protected ChannelStateListener[] stateListeners;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initInternal() {
		Environment env = getEnvironment();
		int numInputs = getNumberOfInputs();
		
		stateListeners = new ChannelStateListener[numInputs];
		for (int i = 0; i < numInputs; i++)
		{
			InputGate<PactRecord> inputGate = (InputGate<PactRecord>) env.getInputGate(i);
			stateListeners[i] = new ChannelStateListener();
			inputGate.subscribeToEvent(stateListeners[i], IterChannelStateEvent.class);
		}
	}

	@Override
	public void invoke() throws Exception {
		stateListeners[0].waitUntil(ChannelState.OPEN);
		
		stateListeners[0].waitUntil(ChannelState.CLOSED);
				
		output.close();
	}

	public void invokeIter() {
		
	}
}
