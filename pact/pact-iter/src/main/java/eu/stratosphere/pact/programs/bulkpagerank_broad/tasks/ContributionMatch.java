package eu.stratosphere.pact.programs.bulkpagerank_broad.tasks;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class ContributionMatch extends MatchStub {
	private PactDouble number = new PactDouble();
	private PactRecord result = new PactRecord();
	
	@Override
	public void match(PactRecord rankState, PactRecord neighbourProb, Collector out)
			throws Exception {
		double rank = rankState.getField(1, number).getValue();
		double prob = neighbourProb.getField(2, number).getValue();
		
		neighbourProb.copyTo(result);
		number.setValue(rank * prob);
		
		result.removeField(0);
		result.setField(1, number);
		out.collect(result);
	}

}
