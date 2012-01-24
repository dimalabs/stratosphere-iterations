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
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

public abstract class IterationHead extends AbstractMinimalTask {
	
	protected static final Log LOG = LogFactory.getLog(IterationHead.class);
	protected static final int INITIAL_QUEUE_SIZE = 100;
	
	public static final String FIXED_POINT_TERMINATOR = "pact.iter.fixedpoint";
	public static final String NUMBER_OF_ITERATIONS = "pact.iter.numiterations";
	
	private ClosedListener channelStateListener;
	private ClosedListener terminationStateListener;
	private long memorySize;
	private int numInternalOutputs;
	
	private volatile boolean finished = false;
	
	@Override
	protected void initTask() {
		boolean useFixedPointTerminator = 
				getRuntimeConfiguration().getBoolean(FIXED_POINT_TERMINATOR, false);
		
		channelStateListener = new ClosedListener();
		getEnvironment().getOutputGate(2).subscribeToEvent(channelStateListener, ChannelStateEvent.class);
		
		if(useFixedPointTerminator) {
			terminationStateListener = new ClosedListener();
			getEnvironment().getOutputGate(3).subscribeToEvent(terminationStateListener, ChannelStateEvent.class);
			numInternalOutputs = 4;
		} else {
			int numIterations = 
					getRuntimeConfiguration().getInteger(NUMBER_OF_ITERATIONS, -1);
			terminationStateListener = new FixedRoundListener(numIterations);
			numInternalOutputs = 3;
		}
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		//Setup variables for easier access to the correct output gates / writers
		//Create output collector for intermediate results
		OutputCollector innerOutput = new OutputCollector();
		RecordWriter<PactRecord>[] innerWriters = getIterationRecordWriters();
		for (RecordWriter<PactRecord> writer : innerWriters) {
			innerOutput.addWriter(writer);
		}
		
		//Create output collector for final iteration output
		OutputCollector taskOutput = new OutputCollector();
		taskOutput.addWriter(output.getWriters().get(0));
		
		//Gates where the iterative channel state is send to
		OutputGate<? extends Record>[] iterStateGates = getIterationOutputGates();
		
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
		AbstractIterativeTask.publishState(ChannelState.OPEN, iterStateGates);

		//Process all input records by passing them to the processInput method (supplied by the user)
		MutableObjectIterator<PactRecord> input = inputs[0];
		processInput(input, innerOutput);
		
		//Send iterative close event to indicate that this round is finished
		AbstractIterativeTask.publishState(ChannelState.CLOSED, iterStateGates);
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
					if (LOG.isInfoEnabled()) {
						LOG.info(constructLogString("Starting Iteration: " + iterationCounter, getEnvironment().getTaskName(), this));
					}
					
					BackTrafficQueueStore.getInstance().publishUpdateQueue(
							getEnvironment().getJobID(), 
							getEnvironment().getIndexInSubtaskGroup(),
							updateQueue.size()>0?updateQueue.size():INITIAL_QUEUE_SIZE,
							this);
					
					//Start new iteration run
					AbstractIterativeTask.publishState(ChannelState.OPEN, iterStateGates);
					
					//Call stub function to process updates
					processUpdates(new QueueIterator(updateQueue), innerOutput);
					
					AbstractIterativeTask.publishState(ChannelState.CLOSED, iterStateGates);
					
					updateQueue = null;
					iterationCounter++;
				}				
			} else {
				throw new RuntimeException("isUpdated() returned false even thoug waitForUpate() exited");
			}
		}
		
		//Call stub so that it can finish its code
		finish(new QueueIterator(updateQueue), taskOutput); 
		
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
	
	public abstract void finish(MutableObjectIterator<PactRecord> iter, OutputCollector output) throws Exception;

	public abstract void processInput(MutableObjectIterator<PactRecord> iter, OutputCollector output) throws Exception;
	
	public abstract void processUpdates(MutableObjectIterator<PactRecord> iter, OutputCollector output) throws Exception;
	
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
	
	private RecordWriter<PactRecord>[] getIterationRecordWriters() {
		int numIterOutputs = this.config.getNumOutputs() - numInternalOutputs;
		
		@SuppressWarnings("unchecked")
		RecordWriter<PactRecord>[] writers = new RecordWriter[numIterOutputs];
		for (int i = 0; i < numIterOutputs; i++) {
			writers[i]  = output.getWriters().get(numInternalOutputs + i);
		}
		
		return writers;
	}
	
	private OutputGate<? extends Record>[] getIterationOutputGates() {
		int numIterOutputs = this.config.getNumOutputs() - numInternalOutputs;
		
		@SuppressWarnings("unchecked")
		OutputGate<? extends Record>[] gates = new OutputGate[numIterOutputs];
		for (int i = 0; i < numIterOutputs; i++) {
			gates[i]  = getEnvironment().getOutputGate(numInternalOutputs + i);
		}
		
		return gates;
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
	
	private class FixedRoundListener extends ClosedListener {
		int numRounds;
		int currentRound;
		
		public FixedRoundListener(int numIterations) {
			this.numRounds = numIterations;
			this.currentRound = 0;
		}

		@Override
		public void waitForUpdate() throws InterruptedException {
			return;
		}
		
		@Override
		public boolean isUpdated() {
			return true;
		}
		
		@Override
		public ChannelState getState() {
			currentRound++;
			if(currentRound == numRounds) {
				return ChannelState.TERMINATED;
			} else {
				return ChannelState.OPEN;
			}
		}
	}
}
