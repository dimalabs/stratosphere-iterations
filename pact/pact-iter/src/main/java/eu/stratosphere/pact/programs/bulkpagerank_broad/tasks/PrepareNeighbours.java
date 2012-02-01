package eu.stratosphere.pact.programs.bulkpagerank_broad.tasks;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class PrepareNeighbours extends AbstractMinimalTask {
	
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		initEnvManagers();
		
		RecordWriter<PactRecord> nodeWriter = output.getWriters().get(0);
		RecordWriter<PactRecord> neighbourWriter = output.getWriters().get(1);
		
		//Send node id for each node and send node / neighbour / partial for each of its neighbours
		PactRecord nodeAdjList = new PactRecord();
		PactLong nodeId = new PactLong();
		LongList neighbourList = new LongList();
		long[] values;
		int numNeighbours;
		PactLong neighbourId = new PactLong();
		PactRecord nodeRecord = new PactRecord();
		PactDouble partial = new PactDouble();
		PactRecord selfLink = new PactRecord();
		PactRecord neighbourLink = new PactRecord();
		PactDouble zero = new PactDouble(0);
		
		while (inputs[0].next(nodeAdjList))
		{
			nodeId = nodeAdjList.getField(0, nodeId);
			neighbourList = nodeAdjList.getField(1, neighbourList);
			numNeighbours = neighbourList.getLength();
			values = neighbourList.getList();
			
			//Send node id
			nodeRecord.setField(0, nodeId);
			nodeWriter.emit(nodeRecord);
			
			//Create special node / node / 0 record so that every node has at least one input
			selfLink.setField(0, nodeId);
			selfLink.setField(1, nodeId);
			selfLink.setField(2, zero);
			neighbourWriter.emit(selfLink);
						
			//Send node/neighbour/partial
			neighbourLink.setField(0, nodeId);
			partial.setValue(1d / numNeighbours);
			for (int i = 0; i < numNeighbours; i++) {
				neighbourId.setValue(values[i]);
				
				neighbourLink.setField(1, neighbourId);
				neighbourLink.setField(2, partial);
				neighbourWriter.emit(neighbourLink);
			}
		}
	}

}
