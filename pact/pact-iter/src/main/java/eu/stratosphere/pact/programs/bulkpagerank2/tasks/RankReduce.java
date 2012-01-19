package eu.stratosphere.pact.programs.bulkpagerank2.tasks;

import java.util.HashSet;
import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class RankReduce extends ReduceStub {
	PactDouble contrib = new PactDouble();
	PactDouble rank = new PactDouble();
	PactString pid = new PactString();
	
	//HACK
	public HashSet<String> pids = new HashSet<String>();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector out)
			throws Exception {
		PactRecord rec = null;
		double contribSum = 0;
		
		while(records.hasNext()) {
			rec = records.next();
			pids.add(rec.getField(0, pid).getValue());
			contribSum += rec.getField(1, contrib).getValue();
		}
		
		if(contribSum != 0) {
			contribSum = 0.15 / 14052 + 0.85 * contribSum;
		} else {
			contribSum = 1d / 14052;
		}
		if(rec.getField(0, pid).getValue().equals("Ελλάδα")) {
			System.out.println("Ελλάδα::"+contribSum);
		}
		
		rank.setValue(contribSum);
		rec.setField(1, rank);
		out.collect(rec);
	}

}
