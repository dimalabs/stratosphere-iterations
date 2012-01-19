package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.util.TerminationDecider;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public class IterationTerminationChecker extends AbstractIterativeTask {
	TerminationDecider decider = null;
	
	public static final String TERMINATION_DECIDER = "iter.termination.decider";
	
	@Override
	protected void initTask() {
		Class<?> cls = getRuntimeConfiguration().getClass(TERMINATION_DECIDER, null);
		try {
			decider = (TerminationDecider) cls.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Could not instantiate termination decider", ex);
		}
	}

	@Override
	public void invoke() throws Exception {
		MutableObjectIterator<PactRecord> input = inputs[0];
		ChannelStateTracker stateListener = stateListeners[0];
		
		List<PactRecord> values = new ArrayList<PactRecord>();
		while(true) {
			try {
				PactRecord rec = new PactRecord();
				boolean success = input.next(rec);				
				if(success) {
					values.add(rec);
				} else {
					//If it returned, but there is no state change the iterator is exhausted 
					// => Finishing
					break;
				}
			} catch (StateChangeException ex) {
				if(stateListener.isChanged() && stateListener.getState() == ChannelState.CLOSED) {
					//Ask oracle whether to stop the program
					boolean terminate = decider.decide(values.iterator());
					values.clear();
					
					if(terminate) {
						getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.TERMINATED));
					} else {
						getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.CLOSED));
					}
				} 
			}
		}
		
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
