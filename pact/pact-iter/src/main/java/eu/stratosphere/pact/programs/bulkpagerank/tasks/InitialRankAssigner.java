package eu.stratosphere.pact.programs.bulkpagerank.tasks;

import java.util.HashSet;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class InitialRankAssigner extends AbstractMinimalTask {
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		PactRecord rec = new PactRecord();
		PactRecord out = new PactRecord();
		PactString pid = new PactString();
		PactDouble initialRank = new PactDouble(1d / 14052);
		
		HashSet<String> pids = new HashSet<String>();
		
		while(inputs[0].next(rec)) {
			pids.add(rec.getField(0, pid).getValue());
		}
		
		for (String pidString : pids) {
			pid.setValue(pidString);
			
			out.setField(0, pid);
			out.setField(1, initialRank);
			output.collect(out);
		}
	}

}
