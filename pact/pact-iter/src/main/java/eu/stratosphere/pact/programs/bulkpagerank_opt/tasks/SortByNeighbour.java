package eu.stratosphere.pact.programs.bulkpagerank_opt.tasks;

import java.util.Comparator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.util.KeyComparator;

public class SortByNeighbour extends AbstractMinimalTask {
	
	private int[] keyPos;
	private Class<? extends Key>[] keyClasses;
	private Comparator<Key>[] comparators;
	
	private UnilateralSortMerger sorter;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		keyPos = config.getLocalStrategyKeyPositions(0);
		keyClasses =  loadKeyClasses();
		
		// create the comparators
		comparators = new Comparator[keyPos.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		initEnvManagers();
		
		try {
			sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 64, comparators, 
					keyPos, keyClasses, inputs[0], this, 0.8f);
		} catch (Exception ex) {
			throw new RuntimeException("Error creating sorter", ex);
		}
		
		MutableObjectIterator<PactRecord> iter = sorter.getIterator();
		
		PactRecord rec = new PactRecord();
		while(iter.next(rec)) {
			output.collect(rec);
		}
	}

}
