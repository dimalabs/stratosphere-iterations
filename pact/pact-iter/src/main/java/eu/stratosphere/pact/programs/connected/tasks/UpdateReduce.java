package eu.stratosphere.pact.programs.connected.tasks;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class UpdateReduce extends ReduceStub {

	@Override
	public void reduce(Iterator<PactRecord> records, Collector out)
			throws Exception {
		PactRecord rec = new PactRecord();
		PactLong cid = new PactLong();
		
		long newCid = Long.MAX_VALUE;
		while(records.hasNext()) {
			rec = records.next();
			cid = rec.getField(1, cid);
			
			if(cid.getValue() < newCid) {
				newCid = cid.getValue();
			}
		}
		
		cid.setValue(newCid);
		rec.setField(1, cid);
		out.collect(rec);
	}
	
	@Override
	public void combine(Iterator<PactRecord> records, Collector out)
			throws Exception {
		reduce(records, out);
		return;
	}

}
