package eu.stratosphere.pact.iterative.nephele.tasks;

import static eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask.initStateTracking;
import static eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask.publishState;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.SerializedPassthroughUpdateBuffer;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;


public class AsynchronousIterationTail extends AbstractMinimalTask {
	
	private static final int DATA_INPUT = 1;
	private static final int PLACEMENT_INPUT = 0;
	private ChannelStateTracker[] stateListeners;

	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		int numInputs = getNumberOfInputs();
		stateListeners = new ChannelStateTracker[numInputs];
		
		for (int i = 0; i < numInputs; i++)
		{
			stateListeners[i] = 
					initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i));
		}
	}
	
	@Override
	public void run() throws Exception {
		//For the iteration internal state tracking, events like iteration close are forwarded using
		//the nephele event mechanisms. The input data for this task should
		//have the same partitioning as the iteration head.
		MutableObjectIterator<Value> input = inputs[DATA_INPUT];
		SerializedPassthroughUpdateBuffer buffer = null;
		DataOutputViewV2 writeOutput = null;
		
		ComponentUpdate rec = new ComponentUpdate();
		while(true) {
			try {
				boolean success = input.next(rec);
				if(success) {
					synchronized (buffer) {
						rec.write(writeOutput);
						buffer.incCount();
						buffer.notifyAll();
					}
				}
				
				//Iterator is exhausted, when channel is closed = FINISHING
				//TODO: Check that iteration state is closed
				if(!success) {
					break;
				}
			} catch (StateChangeException ex) {
				//Can records be lost here which are not yet read??
				if(stateListeners[DATA_INPUT].isChanged()) {
					if(stateListeners[DATA_INPUT].getState() == ChannelState.CLOSED) {
						buffer.flush();
						buffer.close();
						//Feed data into blocking queue, so it unblocks
//						BackTrafficQueueStore.getInstance().publishIterationEnd(
//								getEnvironment().getJobID(),
//								getEnvironment().getIndexInSubtaskGroup(),
//								buffer);
						buffer = null;
						writeOutput = null;
						//Signal synchronization task that we are finished 
						publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
					}
					
					if(stateListeners[DATA_INPUT].getState() == ChannelState.OPEN && buffer == null) {
						//Get new queue to put items into
						buffer = (SerializedPassthroughUpdateBuffer) BackTrafficQueueStore.getInstance().receiveUpdateBuffer(
								getEnvironment().getJobID(),
								getEnvironment().getIndexInSubtaskGroup());
						BackTrafficQueueStore.getInstance().publishIterationEnd(
								getEnvironment().getJobID(),
								getEnvironment().getIndexInSubtaskGroup(),
								buffer);
						writeOutput = buffer.getWriteEnd();
					}
				}
			}
		}
		
		//Read input from second gate so that nephele does not complain about unread
		//channels and it can close this and the previous task.
		inputs[PLACEMENT_INPUT].next(new PactRecord());
		
		output.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}
}