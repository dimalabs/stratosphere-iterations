package eu.stratosphere.pact.iterative.nephele.bulk;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;

public class BulkIterationHead extends IterationHead {

	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		AbstractIterativeTask.publishState(ChannelState.OPEN, getEnvironment().getOutputGate(1));
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			iterOutputWriter.emit(rec);
			output.getWriters().get(1).emit(rec);
		}
		
		AbstractIterativeTask.publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(1));
	}

	@Override
	public void processUpdates(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		processInput(iter);
	}

	@Override
	public void finish() throws Exception {
		//TODO
	}

	@Override
	public void finish(MutableObjectIterator<PactRecord> iter) throws Exception {
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			output.getWriters().get(2).emit(rec);
		}
	}
}
