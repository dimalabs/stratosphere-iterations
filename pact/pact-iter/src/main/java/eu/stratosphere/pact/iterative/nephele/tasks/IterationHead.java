package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore.UpdateQueueStrategy;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public abstract class IterationHead extends AbstractMinimalTask {
	
	protected static final Log LOG = LogFactory.getLog(IterationHead.class);
	protected static final int INITIAL_QUEUE_SIZE = 100;
	
	ClosedListener channelStateListener;
	ClosedListener terminationStateListener;
	long memorySize;
	
	volatile boolean finished = false;
	
	protected OutputGate<? extends Record> iterOutputGate;
	protected OutputGate<? extends Record> terminationOutputGate;
	protected RecordWriter<PactRecord> iterOutputWriter;
	protected RecordWriter<PactRecord> taskOutputWriter;
	protected RecordWriter<PactRecord> terminationOutputWriter;
	
	@Override
	protected void initTask() {
		channelStateListener = new ClosedListener();
		terminationStateListener = new ClosedListener();
		
		getEnvironment().getOutputGate(4).subscribeToEvent(channelStateListener, ChannelStateEvent.class);
		getEnvironment().getOutputGate(5).subscribeToEvent(terminationStateListener, ChannelStateEvent.class);
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		//Setup variables for easier access to the correct output gates / writers
		iterOutputGate = getEnvironment().getOutputGate(0);
		terminationOutputGate = getEnvironment().getOutputGate(5);
		iterOutputWriter = output.getWriters().get(0);
		taskOutputWriter = output.getWriters().get(2);
		terminationOutputWriter = output.getWriters().get(5);
		
		//Create and initialize internal structures for the transport of the iteration
		//updates from the tail to the head (this class)
		BackTrafficQueueStore.getInstance().addStructures(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup(),
				memorySize);
		BackTrafficQueueStore.getInstance().publishUpdateQueue(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup(),
				INITIAL_QUEUE_SIZE,
				this,
				UpdateQueueStrategy.IN_MEMORY_SERIALIZED);
		
		//Start with a first iteration run using the input data
		AbstractIterativeTask.publishState(ChannelState.OPEN, iterOutputGate);

		//Process all input records by passing them to the processInput method (supplied by the user)
		MutableObjectIterator<PactRecord> input = inputs[0];
		processInput(input);
		
		//Send iterative close event to indicate that this round is finished
		AbstractIterativeTask.publishState(ChannelState.CLOSED, iterOutputGate);
		//AbstractIterativeTask.publishState(ChannelState.CLOSED, terminationOutputGate);
		
		//Loop until iteration terminates
		int iterationCounter = 0;
		Queue<PactRecord> updateQueue = null;
		while(true) {
			//Wait until previous iteration run is finished for this subtask
			//and retrieve buffered updates
			try {
				updateQueue = BackTrafficQueueStore.getInstance().receiveIterationEnd(
						getEnvironment().getJobID(), 
						getEnvironment().getIndexInSubtaskGroup());
			} catch (InterruptedException ex) {	
				throw new RuntimeException("Internal Error");
			}
			
			//Wait until also other parallel subtasks have terminated and
			//a termination decision has been made
			try {
				channelStateListener.waitForUpdate();
				terminationStateListener.waitForUpdate();
			} catch (InterruptedException ex) {
				throw new RuntimeException("Internal Error");
			}
			
			
			if(channelStateListener.isUpdated() && terminationStateListener.isUpdated()) {				
				//Check termination criterion
				if(terminationStateListener.getState() == ChannelState.TERMINATED) {
					break;
				} else {
					if (LOG.isInfoEnabled())
						LOG.info(constructLogString("Starting Iteration: " + iterationCounter, getEnvironment().getTaskName(), this));
					
					BackTrafficQueueStore.getInstance().publishUpdateQueue(
							getEnvironment().getJobID(), 
							getEnvironment().getIndexInSubtaskGroup(),
							updateQueue.size()>0?updateQueue.size():INITIAL_QUEUE_SIZE,
							this);
					
					//Start new iteration run
					AbstractIterativeTask.publishState(ChannelState.OPEN, iterOutputGate);
					
					//Call stub function to process updates
					processUpdates(new QueueIterator(updateQueue));
					
					AbstractIterativeTask.publishState(ChannelState.CLOSED, iterOutputGate);
					//AbstractIterativeTask.publishState(ChannelState.CLOSED, terminationOutputGate);
					
					updateQueue = null;
					iterationCounter++;
				}				
			} else {
				throw new RuntimeException("isUpdated() returned false even thoug waitForUpate() exited");
			}
		}
		
		//Call stub so that it can finish its code
		finish(new QueueIterator(updateQueue)); 
		
		//Release the structures for this iteration
		if(updateQueue != null) {
			updateQueue.clear();
		}
		BackTrafficQueueStore.getInstance().releaseStructures(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup());
		
		finished = true;
		//Close output
		output.close();
	}
	
	//TODO: Make abstract and delete old finish
	public void finish(MutableObjectIterator<PactRecord> iter) throws Exception {
	}

	public abstract void processInput(MutableObjectIterator<PactRecord> iter) throws Exception;
	
	public abstract void processUpdates(MutableObjectIterator<PactRecord> iter) throws Exception;
	
	@Deprecated
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
				queue.remove().copyTo(target);
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
					if(state != ChannelState.CLOSED  && state != ChannelState.TERMINATED) {
						throw new RuntimeException("Impossible");
					}
					object.notifyAll();
				}
			}
		}
		
		public void waitForUpdate() throws InterruptedException {
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
		
		public ChannelState getState() {
			return state;
		}
	}
}
