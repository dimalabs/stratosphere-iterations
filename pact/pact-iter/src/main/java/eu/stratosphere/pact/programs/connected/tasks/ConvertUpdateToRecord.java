package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;

public class ConvertUpdateToRecord extends AbstractIterativeTask {

	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		ComponentUpdate update = new ComponentUpdate();
		
		PactRecord rec = new PactRecord();
		PactLong vid = new PactLong();
		PactLong cid = new PactLong();
		
		while(iterationIter.next(update)) {
			vid.setValue(update.getVid());
			cid.setValue(update.getCid());
			rec.setField(0, vid);
			rec.setField(1, cid);
			output.collect(rec);
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
