package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class GroupTask extends AbstractMinimalTask {
	
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
		for (Entry<String, Set<String>> pageGroup : grouped.entrySet()) {
			String page = pageGroup.getKey();
			Set<String> group = pageGroup.getValue();
			
			rec.setField(0, new PactString(page));
			rec.setField(1, new StringSet(group));
			output.collect(rec);
		}
		
		grouped.clear();
		grouped = null;
	}

}
