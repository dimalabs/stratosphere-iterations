package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIteratorV2;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class VertexRankMatchingProbeCaching extends IterationHead {

	protected static final Log LOG = LogFactory.getLog(VertexRankMatchingProbeCaching.class);
	
	private static final int MATCH_CHUNCK_SIZE = 32*1024;

	private VertexPageRank vRank = new VertexPageRank();

	private long cacheMemory;
	private long matchMemory;

	private SpillingResettableMutableObjectIteratorV2 stateIterator;
	private List<MemorySegment> joinMem;
	
	@Override
	protected void initTask() {
		super.initTask();
		
		accessors[1] = new VertexNeighbourPartialAccessor();
		
		cacheMemory = memorySize*2 /3;
		matchMemory = memorySize*1 /3;
	}

	@Override
	public void processInput(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		int chunckSize = MATCH_CHUNCK_SIZE;
		joinMem = 
				memoryManager.allocateStrict(this, (int) (matchMemory/chunckSize), chunckSize);
		
		
		stateIterator = new SpillingResettableMutableObjectIteratorV2<Value>(memoryManager, ioManager, 
				inputs[1], (TypeAccessorsV2<Value>) accessors[1], cacheMemory, this);
		stateIterator.open();
		
		processUpdates(iter, output);
	}

	@Override
	public void processUpdates(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		stateIterator.reset();
		
		VertexNeighbourPartial state = new VertexNeighbourPartial();
		VertexPageRank pageRank = new VertexPageRank();
		VertexPageRank result = new VertexPageRank();
		
		TypeAccessorsV2 buildAccess = new VertexPageRankAccessor();
		TypeAccessorsV2 probeAccess = new VertexNeighbourPartialAccessor();
		TypeComparator comp = new MatchComparator();
		
		MutableHashTable<Value, VertexNeighbourPartial> table = 
				new MutableHashTable<Value, VertexNeighbourPartial>(buildAccess, probeAccess, comp, 
				joinMem, ioManager, 128);
		table.open(iter, EmptyMutableObjectIterator.<VertexNeighbourPartial>get());
		
		int countMatches = 0;
		int countEntries = 0;
		
		while(stateIterator.next(state)) {
			countEntries++;
			HashBucketIterator<Value, VertexNeighbourPartial> tableIter = table.getMatchesFor(state);
			while(tableIter.next(pageRank)) {
				double rank = pageRank.getRank();
				double partial = state.getPartial();
				
				if(Double.isNaN(rank*partial)) {
					LOG.info("NAN: "  + pageRank.getVid() + "::" + rank + " // " + pageRank.getRank() +"::"+ state.getPartial() );
				} else {
					result.setVid(state.getNid());
					result.setRank(rank*partial);
					output.collect(result);
					countMatches++;
				}
			}
		}
		LOG.info("Count entries: " + countEntries);
		LOG.info("Match count: " + countMatches);
		table.close();
	}
	
	@Override
	public int getNumberOfInputs() {
		return 2;
	}
	
	@Override
	public void finish(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		//forwardRecords(iter, output);
	}
	
	private final void forwardRecords(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		while(iter.next(vRank)) {
			output.collect(vRank);
		}
	}
	
	private static final class MatchComparator implements TypeComparator<VertexNeighbourPartial, 
		VertexPageRank>
	{
		private long key;

		@Override
		public void setReference(VertexNeighbourPartial reference, 
				TypeAccessorsV2<VertexNeighbourPartial> accessor) {
			this.key = reference.getVid();
		}

		@Override
		public boolean equalToReference(VertexPageRank candidate, TypeAccessorsV2<VertexPageRank> accessor) {
			return this.key == candidate.getVid();
		}
	}
}
