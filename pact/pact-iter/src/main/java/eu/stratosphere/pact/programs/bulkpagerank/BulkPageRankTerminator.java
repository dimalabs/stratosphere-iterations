package eu.stratosphere.pact.programs.bulkpagerank;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.iterative.nephele.util.TerminationDecider;

public class BulkPageRankTerminator extends TerminationDecider {
	double errorThreshold = 0.000001;
	@Override
	public boolean decide(Iterator<PactRecord> values) {
		while(values.hasNext()) {
			if(values.next().getField(0, PactDouble.class).getValue() > errorThreshold) {
				return false;
			}
		}
		
		return true;
	}

}
