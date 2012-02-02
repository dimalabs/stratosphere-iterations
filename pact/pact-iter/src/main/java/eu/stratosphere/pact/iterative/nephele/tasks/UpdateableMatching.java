package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class UpdateableMatching extends IterationHead {
	
	protected static final Log LOG = LogFactory.getLog(UpdateableMatching.class);

	private int[] buildKeyPos;
	private int[] probeKeyPos;
	private Class<? extends Key>[] keyClasses;
	
	private MutableHashTable<PactRecord, PactRecord> table;
	
	@Override
	protected void initTask() {
		super.initTask();
		
		buildKeyPos = config.getLocalStrategyKeyPositions(0);
		probeKeyPos = config.getLocalStrategyKeyPositions(1);
		keyClasses =  loadKeyClasses();
	}
	
	@Override
	public void finish(MutableObjectIterator<PactRecord> iter,
			OutputCollector output) throws Exception {
		//HashBucketIterator<PactRecord, PactRecord> stateIter = table.getBuildSideIterator();
		
		//PactRecord record = new PactRecord();
		//PactRecord result = new PactRecord();
	}

	@Override
	public void processInput(MutableObjectIterator<PactRecord> iter,
			OutputCollector output) throws Exception {
		initEnvManagers();
		
		// Load build side into table		
		int chunckSize = 32*1024;
		List<MemorySegment> joinMem = memoryManager.allocateStrict(this, (int) (memorySize/chunckSize), chunckSize);
		
		TypeAccessorsV2<PactRecord> buildAccess = new PactRecordAccessorsV2(buildKeyPos, keyClasses);
		TypeAccessorsV2<PactRecord> probeAccess = new PactRecordAccessorsV2(probeKeyPos, keyClasses);
		TypeComparator<PactRecord, PactRecord> comp = new PactRecordComparator();
		
		table = new MutableHashTable<PactRecord, PactRecord>(buildAccess, probeAccess, comp, 
				joinMem, ioManager);
		table.open(inputs[1], EmptyMutableObjectIterator.<PactRecord>get());
		
		// Process input as normally
		processUpdates(iter, output);
	}

	@Override
	public void processUpdates(MutableObjectIterator<PactRecord> iter,
			OutputCollector output) throws Exception {
		PactRecord state = new PactRecord();
		PactRecord probe = new PactRecord();
		PactLong value = new PactLong();
		
		int countUpdated = 0;
		int countUnchanged = 0;
		
		while(iter.next(probe)) {
			HashBucketIterator<PactRecord, PactRecord> tableIter = table.getMatchesFor(probe);
			if(tableIter.next(state)) {
				long oldCid = state.getField(2, value).getValue();
				long updateCid = probe.getField(1, value).getValue();
				
				if(updateCid < oldCid) {
					value.setValue(updateCid);
					state.setField(2, value);
					tableIter.writeBack(state);
					output.collect(state);
					countUpdated++;
				} else {
					countUnchanged++;
				}
			}
			if(tableIter.next(state)) {
				throw new RuntimeException("there should only be one");
			}
		}
		
		LOG.info("Processing stats - Updated: " + countUpdated + " - Unchanged:" + countUnchanged);
	}
	
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	private static final class PactRecordComparator implements TypeComparator<PactRecord, PactRecord>
	{
		private long key;

		@Override
		public void setReference(PactRecord reference, TypeAccessorsV2<PactRecord> accessor) {
			try {
				this.key = reference.getField(0, PactLong.class).getValue();
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}

		@Override
		public boolean equalToReference(PactRecord candidate, TypeAccessorsV2<PactRecord> accessor) {
			try {
				return this.key == candidate.getField(0, PactLong.class).getValue();
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}
	}
}
