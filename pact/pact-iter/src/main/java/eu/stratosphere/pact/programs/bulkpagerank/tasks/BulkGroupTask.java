package eu.stratosphere.pact.programs.bulkpagerank.tasks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class BulkGroupTask extends AbstractMinimalTask {
	
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		MutableObjectIterator<PactRecord> input = inputs[0];
		HashMap<String, Set<String>> grouped = new HashMap<String, Set<String>>();
		PactRecord rec = new PactRecord();
		
		//Group all records
		while(input.next(rec)) {
			String page = rec.getField(0, PactString.class).getValue();
			String link = rec.getField(1, PactString.class).getValue();
			
			Set<String> group = grouped.get(page);
			if(group == null) {
				group = new HashSet<String>();
				grouped.put(page, group);
			}
			
			group.add(link);
		}
		
		//Forward all grouped records
		PactString page = new PactString();
		PactString tid = new PactString();
		PactDouble contribution = new PactDouble();
		for (Entry<String, Set<String>> pageGroup : grouped.entrySet()) {
			page.setValue(pageGroup.getKey());
			Set<String> neighbours = pageGroup.getValue();
			contribution.setValue(1d / neighbours.size());
			
			for (String neighbour : neighbours) {
				tid.setValue(neighbour);
				rec.setField(0, page);
				rec.setField(1, tid);
				rec.setField(2, contribution);
				output.collect(rec);
			}
		}
		
		grouped.clear();
		grouped = null;
	}

}
