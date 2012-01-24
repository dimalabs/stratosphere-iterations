package eu.stratosphere.pact.programs.bulkpagerank_opt.tasks;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class RankReduce extends ReduceStub {
	PactDouble contrib = new PactDouble();
	PactDouble rank = new PactDouble();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector out)
			throws Exception {
		PactRecord rec = null;
		
		double contribSum = 0;
		while(records.hasNext()) {
			rec = records.next();
			contribSum += rec.getField(1, contrib).getValue();
		}
		
		contribSum = 0.15 / 14052 + 0.85 * contribSum;
		
		rank.setValue(contribSum);
		rec.setField(1, rank);
		out.collect(rec);
	}

}
