package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class SendUpdates extends AbstractIterativeTask {
	PactRecord record = new PactRecord();
	PactRecord result = new PactRecord();
	LongList neighbours = new LongList();
	PactLong nid = new PactLong();
	PactLong cid = new PactLong();
	
	@Override
	public void invokeStart() throws Exception {
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		while(iterationIter.next(record)) {
			cid = record.getField(2, cid);
			neighbours = record.getField(1, neighbours);
			
			int numNeighbours = neighbours.getLength();
			long[] neighbourIds = neighbours.getList();
			
			result.setField(1, cid);
			for (int i = 0; i < numNeighbours; i++) {
				nid.setValue(neighbourIds[i]);
				result.setField(0, nid);
				output.collect(result);
			}
		}
	}

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
