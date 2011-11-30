package eu.stratosphere.pact.iterative.nephele.tasks.core;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.tasks.util.PactRecordEvent;

public abstract class IterationHead extends AbstractMinimalTask {
	
	PactRecordEventListener recordListener;
	ClosedListener channelStateListener;
	Thread executionThread;
	
	volatile boolean finished = false;
	
	@Override
	protected void initTask() {
		recordListener = new PactRecordEventListener();
		channelStateListener = new ClosedListener();
		
		getEnvironment().getOutputGate(1).subscribeToEvent(recordListener, PactRecordEvent.class);
		getEnvironment().getOutputGate(1).subscribeToEvent(channelStateListener, ChannelStateEvent.class);
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		//Send iterative open event
		AbstractIterativeTask.publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));

		//Forward input to stub function
		MutableObjectIterator<PactRecord> input = inputs[0];
		processInput(input);
		
		//Send iterative close event
		AbstractIterativeTask.publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
		
		//Loop until iteration terminates
		executionThread = Thread.currentThread();
		int iterationCounter = 0;
		
		while(iterationCounter < 5) {
			//Wait until previous iteration run is finished
			try {
				channelStateListener.waitForUpate();
			} catch (InterruptedException ex) {
				
			}
			
			if(channelStateListener.isUpdated()) {
				//Retrieve buffered updates
				Queue<PactRecord> queue = recordListener.getQueue();
				//Create new queue for further updates
				recordListener.useNewQueue();
				
				//Start new iteration run
				AbstractIterativeTask.publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));
				
				//Call stub function to process updates
				processUpdates(new QueueIterator(queue));
				
				AbstractIterativeTask.publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
				iterationCounter++;
			} else {
				throw new RuntimeException("Should never happen!");
			}
		}
		
		finished = true;
		//Close output
		output.close();
	}
	
	public abstract void processInput(MutableObjectIterator<PactRecord> iter) throws Exception;
	
	public abstract void processUpdates(MutableObjectIterator<PactRecord> iter) throws Exception;

	private static class PactRecordEventListener implements EventListener {
		Queue<PactRecord> queue = new LinkedList<PactRecord>();
		
		@Override
		public void eventOccurred(AbstractTaskEvent event) {
			PactRecordEvent evt = (PactRecordEvent) event;
			queue.add(evt.getRecord());
		}

		public void useNewQueue() {
			queue = new LinkedList<PactRecord>();
		}

		public Queue<PactRecord> getQueue() {
			return queue;
		}
		
	}
	
	private static class QueueIterator implements MutableObjectIterator<PactRecord> {
		Queue<PactRecord> queue;
		
		public QueueIterator(Queue<PactRecord> queue) {
			this.queue = queue;
		}

		@Override
		public boolean next(PactRecord target) throws IOException {
			if(!queue.isEmpty()) {
				queue.poll().copyTo(target);
				return true;
			} else {
				return false;
			}
		}
		
	}
	
	private class ClosedListener implements EventListener {
		volatile Object object = new Object();
		volatile boolean updated = false;
		
		@Override
		public void eventOccurred(AbstractTaskEvent event) {
			if(!finished) {
				synchronized(object) {
					updated = true;
					object.notifyAll();
				}
			}
		}
		
		public void waitForUpate() throws InterruptedException {
			synchronized (object) {
				while(!updated) {
					object.wait();
				}
			}
		}
		
		public boolean isUpdated() {
			if(updated) {
				updated = false;
				return true;
			}
			
			return false;
		}
	}
}
