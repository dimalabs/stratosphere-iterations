package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;

public class ConvertRecordToUpdate extends AbstractIterativeTask {

	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		ComponentUpdate update = new ComponentUpdate();
		
		PactRecord rec = new PactRecord();
		PactLong vid = new PactLong();
		PactLong cid = new PactLong();
		
		while(iterationIter.next(rec)) {
			vid = rec.getField(0, vid);
			cid = rec.getField(1, cid);
			
			update.setVid(vid.getValue());
			update.setCid(cid.getValue());
			output.collect(update);
		}
	}

	@Override
	protected void initTask() {
		outputAccessors[0] = new ComponentUpdateAccessor();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
