package eu.stratosphere.pact.programs.connected.tasks;

import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class UpdateableBuildCachingMatch extends AbstractIterativeTask {
	
	private SpillingResettableMutableObjectIterator buildIter;
	private int[] buildKeyPos;
	private int[] probeKeyPos;
	private Class<? extends Key>[] keyClasses;
	
	private MatchStub stub;
	private MutableHashTable<PactRecord, PactRecord> table;
	
	@Override
	protected void initTask() {
		buildKeyPos = config.getLocalStrategyKeyPositions(0);
		probeKeyPos = config.getLocalStrategyKeyPositions(1);
		keyClasses =  loadKeyClasses();
		
		stub = initStub(MatchStub.class);
	}
	
	@Override
	public void invokeStart() throws Exception {
		initEnvManagers();
		
		int chunckSize = 2*1024*1024;
		List<MemorySegment> joinMem = memoryManager.allocateStrict(this, (int) (memorySize/chunckSize), chunckSize);
		
		TypeAccessorsV2<PactRecord> buildAccess = new PactRecordAccessorsV2(buildKeyPos, keyClasses);
		TypeAccessorsV2<PactRecord> probeAccess = new PactRecordAccessorsV2(probeKeyPos, keyClasses);
		TypeComparator<PactRecord, PactRecord> comp = new PactRecordComparator();
		
		table = new MutableHashTable<PactRecord, PactRecord>(buildAccess, probeAccess, comp, 
				joinMem, ioManager);
		table.open(inputs[1], EmptyMutableObjectIterator.<PactRecord>get());
	}
	
	@Override
	public void invokeIter(IterationIterator probeIter) throws Exception {
		PactRecord state = new PactRecord();
		PactRecord probe = new PactRecord();
		PactLong value = new PactLong();
		
		while(probeIter.next(probe)) {
			HashBucketIterator<PactRecord, PactRecord> iter = table.getMatchesFor(probe);
			if(iter.next(state)) {
				long oldCid = state.getField(2, value).getValue();
				long updateCid = probe.getField(1, value).getValue();
				
				if(updateCid < oldCid) {
					value.setValue(updateCid);
					state.setField(2, value);
					iter.writeBack(state);
					output.collect(state);
				}
			}
			if(iter.next(state)) {
				throw new RuntimeException("there should only be one");
			}
		}
	}
	
	@Override
	public void cleanup() throws Exception {
		buildIter.close();
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
