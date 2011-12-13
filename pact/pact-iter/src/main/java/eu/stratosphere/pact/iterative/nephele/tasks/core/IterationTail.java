package eu.stratosphere.pact.iterative.nephele.tasks.core;

import java.util.Queue;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;


public class IterationTail extends AbstractIterativeTask {

	@Override
	public void invoke() throws Exception {
		//For the iteration internal state tracking, events like iteration close are forwarded using
		//the nephele event mechanisms. The input data for this task should
		//have the same partitioning as the iteration head.
		MutableObjectIterator<PactRecord> input = inputs[0];
		Queue<PactRecord> queue = null;
		
		PactRecord rec = new PactRecord();
		while(true) {
			try {
				boolean success = input.next(rec);
				if(success) {
					//Add record to queue for next iteration run
					queue.add(rec);
				}
				
				//Iterator is exhausted, when channel is closed = FINISHING
				//TODO: Check that iteration state is closed
				if(!success) {
					break;
				}
			} catch (StateChangeException ex) {
				//Can records be lost here which are not yet read??
				if(stateListeners[0].isChanged()) {
					if(stateListeners[0].getState() == ChannelState.CLOSED) {
						//Feed data into blocking queue, so it unblocks
						BackTrafficQueueStore.getInstance().publishIterationEnd(
								getEnvironment().getJobID(),
								getEnvironment().getIndexInSubtaskGroup(),
								queue);
						queue = null;
						//Signal synchronization task that we are finished 
						publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(1));
					}
					
					if(stateListeners[0].getState() == ChannelState.OPEN && queue == null) {
						//Get new queue to put items into
						queue = BackTrafficQueueStore.getInstance().receiveUpdateQueue(
								getEnvironment().getJobID(),
								getEnvironment().getIndexInSubtaskGroup());
					}
				}
			}
		}
		
		//Read input from second gate so that nephele does not complain about unread
		//channels and it can close this and the previous task.
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