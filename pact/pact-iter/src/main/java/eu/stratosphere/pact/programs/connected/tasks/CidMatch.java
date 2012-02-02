package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class CidMatch extends MatchStub {

	PactLong value;
	
	@Override
	public void match(PactRecord state, PactRecord update, Collector out)
			throws Exception {
		long oldCid = state.getField(2, value).getValue();
		long updateCid = update.getField(1, value).getValue();
		
		if(updateCid < oldCid) {
			value.setValue(updateCid);
			state.setField(2, value);
			out.collect(state);
		}
	}

}