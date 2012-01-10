package eu.stratosphere.pact.iterative.nephele.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;

public class TenRoundTermination extends TerminationDecider {
	int counter = 0;
	
	@Override
	public boolean decide(Iterator<PactRecord> values) {
		counter++;
		if(counter <= 10) {
			return false;
		} else {
			return true;
		}
	}

}
