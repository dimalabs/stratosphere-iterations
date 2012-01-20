package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;

public class SortingPageRankIteration extends IterationHead {

	private static final int NUM_VERTICES = 14052;
	
	private String[] pages;
	private Double[] ranks;
	private String[][] adjList;
	
	//private int numVertices = NUM_VERTICES;
	private int numLocalVertices = -1;
	
	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter)
			throws Exception {
		SortedMap<String, String[]> sortingNeighbourMap = new TreeMap<String, String[]>();
		
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			String page = rec.getField(0, PactString.class).getValue();
			
			Set<String> outgoingSet = rec.getField(1, StringSet.class).getValue();
			String[] outgoing = new String[outgoingSet.size()];
			
			int i = 0;
			for (String neighbour : outgoingSet) {
				outgoing[i++] = neighbour;
			}
			
			sortingNeighbourMap.put(page, outgoing);
		}
		
		numLocalVertices = sortingNeighbourMap.size();
		
		pages = new String[numLocalVertices];
		ranks = new Double[numLocalVertices];
		adjList = new String[numLocalVertices][];
		
		Double initialRank = 1.0 / NUM_VERTICES;
		
		int i= 0;
		for (Entry<String, String[]> entry : sortingNeighbourMap.entrySet()) {
			pages[i] = entry.getKey();
			ranks[i] = initialRank;
			adjList[i] = entry.getValue();
			
			String[] outgoing = adjList[i];
			PactDouble prank = new PactDouble(ranks[i] / outgoing.length);
			for (String neighbour : outgoing) {
				rec.setField(0, new PactString(neighbour));
				rec.setField(1, prank);
				innerOutputWriter.emit(rec);
			}
			i++;
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
		
		TreeMap<String, Double> sortedUpdates = new TreeMap<String, Double>(sumMap);
		sumMap = null; //Help garbage collection
		
		double maxDiff = Double.NEGATIVE_INFINITY;
		
		Iterator<Entry<String, Double>> sortedUpdatesIter = sortedUpdates.entrySet().iterator();
		if(sortedUpdatesIter.hasNext()) {
			Entry<String, Double> currentUpdate = sortedUpdatesIter.next();
			String updatePage = currentUpdate.getKey();
			
			for (int i = 0; i < pages.length; i++) {
				String page = pages[i];
				int comp;
				while((comp = page.compareTo(updatePage)) > 0) {
					if(sortedUpdatesIter.hasNext()) {
						currentUpdate = sortedUpdatesIter.next();
						updatePage = currentUpdate.getKey();
					} else {
						break;
					}
				}
				
				if(comp == 0) {
					double normalizedRank = 0.15 / NUM_VERTICES + 0.85 * currentUpdate.getValue();
					double diff = Math.abs(normalizedRank - ranks[i]);
					if(diff > maxDiff) {
						maxDiff = diff;
					}
					ranks[i] = normalizedRank;
					
					if(sortedUpdatesIter.hasNext()) {
						currentUpdate = sortedUpdatesIter.next();
						updatePage = currentUpdate.getKey();
					} else {
						break;
					}
				}
			}	
		}
		
		sortedUpdates = null; //Help garbage collection
		
		for (int i = 0; i < numLocalVertices; i++) {
			double rank = ranks[i];
			String[] outgoing = adjList[i];
			
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
		
		for (int i = 0; i < numLocalVertices; i++) {
			String page = pages[i];
			double rank = ranks[i];
			
			rec.setField(0, new PactString(page));
			rec.setField(1, new PactDouble(rank));
			taskOutputWriter.emit(rec);
		}
	}

}
