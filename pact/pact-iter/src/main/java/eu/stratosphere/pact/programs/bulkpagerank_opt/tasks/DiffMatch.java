package eu.stratosphere.pact.programs.bulkpagerank_opt.tasks;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class DiffMatch extends MatchStub {
	PactDouble number = new PactDouble();
	PactRecord result = new PactRecord();
	
	@Override
	public void match(PactRecord oldRankState, PactRecord newRankState, Collector out)
			throws Exception {
		double oldRank = oldRankState.getField(1, number).getValue();
		double newRank = newRankState.getField(1, number).getValue();
		double diff = Math.abs(newRank-oldRank);
		
		number.setValue(diff);
		
		result.setField(0, number);
		out.collect(result);
	}
	
}
