package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class InitialUpdates extends AbstractMinimalTask {

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		PactRecord record = new PactRecord();
		PactRecord result = new PactRecord();
		
		PactLong id = new PactLong();
		PactLong oldCid = new PactLong();
		LongList neighbours = new LongList();
		PactLong newCid = new PactLong();
		
		while (inputs[0].next(record))
		{	
			id = record.getField(0, id);
			oldCid = record.getField(2, oldCid);
			
			//Look if neighbour is smaller
			long cid = oldCid.getValue();
			neighbours = record.getField(1, neighbours);
			int numNeighbours = neighbours.getLength();
			long[] neighbourIds = neighbours.getList();
			for (int i = 0; i < numNeighbours; i++) {
				if(neighbourIds[i] < cid) {
					cid = neighbourIds[i];
				}
			}
			newCid.setValue(cid);
			
			if(newCid.getValue() < oldCid.getValue()) {
				result.setField(0, id);
				result.setField(1, newCid);
				output.collect(result);
			}
		}
	}

}
