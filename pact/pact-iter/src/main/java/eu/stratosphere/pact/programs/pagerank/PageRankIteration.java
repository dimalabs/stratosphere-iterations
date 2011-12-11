package eu.stratosphere.pact.programs.pagerank;

import java.util.HashMap;
import java.util.Set;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationHead;

public class PageRankIteration extends IterationHead {

	private static final int NUM_VERTICES = 14052;
	
	private HashMap<String, Set<String>> adjList;
	private HashMap<String, Double> ranks;
	
	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		adjList = new HashMap<String, Set<String>>(NUM_VERTICES);
		ranks = new HashMap<String, Double>(NUM_VERTICES);
		
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			String page = rec.getField(0, PactString.class).getValue();
			double rank = 1d / NUM_VERTICES;
			Set<String> outgoing = rec.getField(1, StringSet.class).getValue();
			
			adjList.put(page, outgoing);
			ranks.put(page, rank);
		}
		
		for (String page : adjList.keySet()) {
			Set<String> outgoing = adjList.get(page);
			
			PactDouble prank = new PactDouble(ranks.get(page) / outgoing.size());
			for (String neighbour : outgoing) {
				rec.setField(0, new PactString(neighbour));
				rec.setField(1, prank);
				output.getWriters().get(0).emit(rec);
			}
		}		
	}

	@Override
	public void processUpdates(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		HashMap<String, Double> sumMap = new HashMap<String, Double>();
		
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			String page = rec.getField(0, PactString.class).getValue();
			double rank = rec.getField(1, PactDouble.class).getValue();
			
			if(!sumMap.containsKey(page)) {
				sumMap.put(page, rank);
			} else {
				sumMap.put(page, sumMap.get(page) + rank);
			}
		}
		
		for (String page : sumMap.keySet()) {
			double normalizedRank = 0.15 / NUM_VERTICES + 0.85 * sumMap.get(page);
			ranks.put(page, normalizedRank);
		}
		
		for (String page : adjList.keySet()) {
			//if(!sumMap.containsKey(page)) {
			//	continue;
			//}
			
			double rank = ranks.get(page);
			Set<String> outgoing = adjList.get(page);
			
			PactDouble prank = new PactDouble(rank / outgoing.size());
			for (String neighbour : outgoing) {
				rec.setField(0, new PactString(neighbour));
				rec.setField(1, prank);
				output.getWriters().get(0).emit(rec);
			}
		}
	}

	@Override
	public void finish() throws Exception {
		PactRecord rec = new PactRecord();
		
		for (String page : adjList.keySet()) {
			double rank = ranks.get(page);
			
			rec.setField(0, new PactString(page));
			rec.setField(1, new PactDouble(rank));
			output.getWriters().get(1).emit(rec);
		}
	}

}
