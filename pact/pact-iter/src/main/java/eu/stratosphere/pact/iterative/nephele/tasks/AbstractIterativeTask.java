package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.NepheleUtil;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public abstract class AbstractIterativeTask extends AbstractMinimalTask {
	
	protected ChannelStateTracker[] stateListeners;
	protected MemoryManager memoryManager;
	protected IOManager ioManager;
	protected TaskConfig config;
	
	protected int memorySize;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initInternal() {
		Environment env = getEnvironment();
		memoryManager = env.getMemoryManager();
		ioManager = env.getIOManager();
		int numInputs = getNumberOfInputs();
		memorySize = getRuntimeConfiguration().getInteger(NepheleUtil.TASK_MEMORY, 0) * 1024 * 1024;
		config = new TaskConfig(getRuntimeConfiguration());
		
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
		
		boolean firstRound = true;
		
		IterationIterator iterationIter = new IterationIterator(input, stateListener);
		while(!iterationIter.checkTermination()) {
			//Send iterative open state to output gates
			publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));
			
			if(firstRound) {
				invokeStart();
				firstRound = false;
			}
			
			//Call iteration stub function with the data for this iteration
			invokeIter(iterationIter);
			
			if(stateListener.getState() == ChannelState.CLOSED) {
				publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
			} else {
				throw new RuntimeException("Illegal state after iteration call");
			}
		}
		
		invokeEnd();
		
		output.close();
	}
	
	/**
	 * Allows to run code that should only be run once in the beginning
	 * @throws Exception
	 */
	public void invokeStart() throws Exception {
		
	}
	public void invokeEnd() throws Exception {
		
	}

	public abstract void invokeIter(IterationIterator iterationIter) throws Exception;
	
	protected void initEnvManagers() {
		Environment env = getEnvironment();
		memoryManager = env.getMemoryManager();
		ioManager = env.getIOManager();
	}
	
	public static void publishState(ChannelState state, OutputGate<?> gate) throws Exception {
		gate.flush(); //So that all records are hopefully send in a seperate envelope from the event
		//Now when the event arrives, all records will be processed before the state change.
		gate.publishEvent(new ChannelStateEvent(state));
		gate.flush();
	}	
}
