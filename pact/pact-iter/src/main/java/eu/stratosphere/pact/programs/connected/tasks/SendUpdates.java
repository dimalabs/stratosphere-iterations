package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;

public class SendUpdates extends AbstractIterativeTask {
	TransitiveClosureEntry tc = new TransitiveClosureEntry();
	ComponentUpdate update = new ComponentUpdate();
	
	@Override
	public void invokeStart() throws Exception {
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		outputAccessors[0] = new ComponentUpdateAccessor();
		setOutputAccessors();
		
		while(iterationIter.next(tc)) {
			long cid = tc.getCid();
			
			int numNeighbours = tc.getNumNeighbors();
			long[] neighbourIds = tc.getNeighbors();
			
			update.setCid(cid);
			for (int i = 0; i < numNeighbours; i++) {
				update.setVid(neighbourIds[i]);
				output.collect(update);
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
