package eu.stratosphere.pact.programs.bulkpagerank.tasks;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class ContributionMatch extends MatchStub {
	private PactDouble number = new PactDouble();
	private PactString tid = new PactString();
	private PactDouble contrib = new PactDouble();
	
	@Override
	public void match(PactRecord rankState, PactRecord neighbourProb, Collector out)
			throws Exception {
		double rank = rankState.getField(1, number).getValue();
		double prob = neighbourProb.getField(2, number).getValue();
		tid = neighbourProb.getField(1, tid);
		
		contrib.setValue(rank * prob);
		
		rankState.setField(0, tid);
		rankState.setField(1, contrib);
		out.collect(rankState);
	}

}
