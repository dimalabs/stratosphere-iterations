package eu.stratosphere.pact.iterative.nephele.tasks.core;

import java.io.IOException;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public abstract class IterationHead extends AbstractMinimalTask {
	
	protected static final Log LOG = LogFactory.getLog(IterationHead.class);
	
	ClosedListener channelStateListener;
	
	volatile boolean finished = false;
	
	@Override
	protected void initTask() {
		channelStateListener = new ClosedListener();
		
		getEnvironment().getOutputGate(3).subscribeToEvent(channelStateListener, ChannelStateEvent.class);
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		BackTrafficQueueStore.getInstance().addStructures(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup());
		BackTrafficQueueStore.getInstance().publishUpdateQueue(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup(),
				100);
		
		//Start with a first iteration run using the input data
		AbstractIterativeTask.publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));

		//Process all input records by passing them to the processInput method (supplied by the user)
		MutableObjectIterator<PactRecord> input = inputs[0];
		processInput(input);
		
		//Send iterative close event to indicate that this round is finished
		AbstractIterativeTask.publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
		
		//Loop until iteration terminates
		int iterationCounter = 0;
		while(true) {
			Queue<PactRecord> updateQueue = null;
			//Wait until previous iteration run is finished for this subtask
			//and retrieve buffered updates
			try {
				updateQueue = BackTrafficQueueStore.getInstance().receiveIterationEnd(
						getEnvironment().getJobID(), 
						getEnvironment().getIndexInSubtaskGroup());
			} catch (InterruptedException ex) {	
				throw new RuntimeException("Internal Error");
			}
			
			//Wait until all other parallel subtasks have terminated
			try {
				channelStateListener.waitForClose();
			} catch (InterruptedException ex) {
				throw new RuntimeException("Internal Error");
			}
			
			
			if(channelStateListener.isUpdated()) {
				BackTrafficQueueStore.getInstance().publishUpdateQueue(
						getEnvironment().getJobID(), 
						getEnvironment().getIndexInSubtaskGroup(),
						updateQueue.size()>0?updateQueue.size():100);
				
				//Check termination criterion
				if(iterationCounter == 10) {
					updateQueue = null;
					break;
				} else {
					if (LOG.isInfoEnabled())
						LOG.info(constructLogString("Starting Iteration: " + iterationCounter, getEnvironment().getTaskName(), this));
					
					//Start new iteration run
					AbstractIterativeTask.publishState(ChannelState.OPEN, getEnvironment().getOutputGate(0));
					
					//Call stub function to process updates
					processUpdates(new QueueIterator(updateQueue));
					
					AbstractIterativeTask.publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(0));
					
					updateQueue = null;
					iterationCounter++;
				}				
			} else {
				throw new RuntimeException("isUpdated() returned false even thoug waitForUpate() exited");
			}
		}
		
		//Call stub so that it can finish its code
		finish(); 
		
		finished = true;
		//Close output
		output.close();
	}
	
	public abstract void processInput(MutableObjectIterator<PactRecord> iter) throws Exception;
	
	public abstract void processUpdates(MutableObjectIterator<PactRecord> iter) throws Exception;
	
	public abstract void finish() throws Exception;
	
	public static String constructLogString(String message, String taskName, AbstractInvokable parent)
	{
		StringBuilder bld = new StringBuilder(128);	
		bld.append(message);
		bld.append(':').append(' ');
		bld.append(taskName);
		bld.append(' ').append('(');
		bld.append(parent.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(parent.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
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
		volatile ChannelState state;
		volatile boolean updated = false;
		
		@Override
		public void eventOccurred(AbstractTaskEvent event) {
			if(!finished) {
				synchronized(object) {
					updated = true;
					state = ((ChannelStateEvent) event).getState();
					if(state != ChannelState.CLOSED) {
						throw new RuntimeException("Impossible");
					}
					object.notifyAll();
				}
			}
		}
		
		public void waitForClose() throws InterruptedException {
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
