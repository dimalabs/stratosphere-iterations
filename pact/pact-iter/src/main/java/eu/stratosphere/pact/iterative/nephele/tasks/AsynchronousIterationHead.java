package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.BackTrafficQueueStore;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.DeserializingIterator;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.iterative.nephele.util.SerializedPassthroughUpdateBuffer;
import eu.stratosphere.pact.iterative.nephele.util.SerializedUpdateBuffer;

public abstract class AsynchronousIterationHead extends IterationHead {

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {		
		//Setup variables for easier access to the correct output gates / writers
		//Create output collector for intermediate results
		OutputCollectorV2 innerOutput = new OutputCollectorV2();
		RecordWriter<Value>[] innerWriters = getIterationRecordWriters();
		for (RecordWriter<Value> writer : innerWriters) {
			innerOutput.addWriter(writer);
		}
		
		//Create output collector for final iteration output
		OutputCollectorV2 taskOutput = new OutputCollectorV2();
		taskOutput.addWriter(output.getWriters().get(0));
		
		//Gates where the iterative channel state is send to
		OutputGate<? extends Record>[] iterStateGates = getIterationOutputGates();
		
		//Allocate memory for update queue
		List<MemorySegment> updateMemory = memoryManager.allocateStrict(this,
				(int)(updateBufferSize / MEMORY_SEGMENT_SIZE), MEMORY_SEGMENT_SIZE);
		SerializedPassthroughUpdateBuffer buffer = 
				new SerializedPassthroughUpdateBuffer(updateMemory, MEMORY_SEGMENT_SIZE);
		
		//Create and initialize internal structures for the transport of the iteration
		//updates from the tail to the head (this class)
		BackTrafficQueueStore.getInstance().addStructures(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup());
		BackTrafficQueueStore.getInstance().publishUpdateBuffer(
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup(),
				buffer);
		
		//Start with a first iteration run using the input data
		AbstractIterativeTask.publishState(ChannelState.OPEN, iterStateGates);

		//Process all input records by passing them to the processInput method (supplied by the user)
		MutableObjectIterator<Value> input = inputs[0];
		processInput(new WrappedIterator(input, 
				getEnvironment().getJobID(), 
				getEnvironment().getIndexInSubtaskGroup(), buffer), innerOutput);
		
		AbstractIterativeTask.publishState(ChannelState.CLOSED, iterStateGates);
		
		Thread.sleep(2000);
		memoryManager.release(updateMemory);
		
		finished = true;
	}
	
	protected static class WrappedIterator implements MutableObjectIterator<Value> {

		private JobID id;
		private int subtaskIndex;
		private MutableObjectIterator<Value> initialIter;
		private boolean second = false;
		private SerializedUpdateBuffer updatesBuffer;
		private DeserializingIterator updatesIter;
		private SerializedPassthroughUpdateBuffer buffer;
		
		public WrappedIterator(MutableObjectIterator<Value> initialIter, JobID id, int subtaskIndex,
				SerializedPassthroughUpdateBuffer buffer) {
			this.initialIter = initialIter;
			this.id = id;
			this.subtaskIndex = subtaskIndex;
			this.buffer = buffer;
		}
		@Override
		public boolean next(Value target) throws IOException {
			if(!second) {
				boolean success = initialIter.next(target);
				if(success) {
					return true;
				} else {
					second = true;
					
					try {
						updatesBuffer = BackTrafficQueueStore.getInstance().receiveIterationEnd(
								id, subtaskIndex);
						updatesIter = new DeserializingIterator(updatesBuffer.getReadEnd());
					} catch (InterruptedException e) {
						throw new RuntimeException("The house is on fire!");
					}
				}
			}
			
			if(updatesIter.next(target)) {
				buffer.decCount();
			} else if(buffer.getCount() != 0) {
				throw new RuntimeException("Could not read but there should be messages: " + buffer.getCount());
			} else {
				return false;
			}
			//No else on purpose 
			return true;
		}
		
	}
	@Override
	public void cleanup() throws Exception {
		// TODO Auto-generated method stub
		//super.cleanup();
	}
}
