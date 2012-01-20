package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.HashMap;
import java.util.Set;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;

public class HashingPageRankIteration extends IterationHead {

	private static final int NUM_VERTICES = 14052;
	
	private HashMap<String, String[]> adjList;
	private HashMap<String, Double> ranks;
	
	private int numLocalVertices = -1;
	
	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		adjList = new HashMap<String, String[]>(NUM_VERTICES/getEnvironment().getCurrentNumberOfSubtasks());
		
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			String page = rec.getField(0, PactString.class).getValue();
			
			Set<String> outgoingSet = rec.getField(1, StringSet.class).getValue();
			String[] outgoing = new String[outgoingSet.size()];
			
			int i = 0;
			for (String neighbour : outgoingSet) {
				outgoing[i++] = neighbour;
			}
			
			adjList.put(page, outgoing);
		}
		
		numLocalVertices = adjList.size();
		ranks = new HashMap<String, Double>(numLocalVertices);
		double initialRank = 1.0 / NUM_VERTICES;
		
		for (String page : adjList.keySet()) {
			ranks.put(page, initialRank);
			
			String[] outgoing = adjList.get(page);
			PactDouble prank = new PactDouble(ranks.get(page) / outgoing.length);
			for (String neighbour : outgoing) {
				rec.setField(0, new PactString(neighbour));
				rec.setField(1, prank);
				innerOutputWriter.emit(rec);
			}
		}
		
		PactRecord errRecord = new PactRecord();
		errRecord.setField(0, new PactDouble(initialRank));
		terminationOutputWriter.emit(errRecord);
	}

	@Override
	public void processUpdates(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		HashMap<String, Double> sumMap = new HashMap<String, Double>(numLocalVertices);
		
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
		
		double maxDiff = Double.NEGATIVE_INFINITY;
		
		for (String page : sumMap.keySet()) {
			double normalizedRank = 0.15 / NUM_VERTICES + 0.85 * sumMap.get(page);
			Double oldRank = ranks.get(page);
			if(oldRank != null) {
				double diff = Math.abs(normalizedRank - oldRank);
				if(diff > maxDiff) {
					maxDiff = diff;
				}
			}
			
			ranks.put(page, normalizedRank);
		}
		sumMap = null; //Help garbage collection
		
		for (String page : adjList.keySet()) {			
			double rank = ranks.get(page);
			String[] outgoing = adjList.get(page);
			
			PactDouble prank = new PactDouble(rank / outgoing.length);
			for (String neighbour : outgoing) {
				rec.setField(0, new PactString(neighbour));
				rec.setField(1, prank);
				innerOutputWriter.emit(rec);
			}
		}
		
		PactRecord errRecord = new PactRecord();
		errRecord.setField(0, new PactDouble(maxDiff));
		terminationOutputWriter.emit(errRecord);
	}

	@Override
	public void finish() throws Exception {
		PactRecord rec = new PactRecord();
		
		for (String page : adjList.keySet()) {
			double rank = ranks.get(page);
			
			rec.setField(0, new PactString(page));
			rec.setField(1, new PactDouble(rank));
			taskOutputWriter.emit(rec);
		}
	}

}
