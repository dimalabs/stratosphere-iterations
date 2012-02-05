package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.Comparator;

import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.PactRecordCollector;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public class SortingReduce extends AbstractIterativeTask {

	private int[] keyPos;
	private Class<? extends Key>[] keyClasses;
	private Comparator<Key>[] comparators;
	
	private ReduceStub stub;
	private UnilateralSortMerger sorter;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		keyPos = config.getLocalStrategyKeyPositions(0);
		keyClasses =  loadKeyClasses();
		
		stub = initStub(ReduceStub.class);
		
		// create the comparators
		comparators = new Comparator[keyPos.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}
	}
	
	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		MutableObjectIterator typeHidingIter = iterationIter;
		try {
			sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 64, comparators, 
					keyPos, keyClasses, typeHidingIter, this, 0.8f);
		} catch (Exception ex) {
			throw new RuntimeException("Error creating sorter", ex);
		}
		
		KeyGroupedIterator iter = new KeyGroupedIterator(sorter.getIterator(), keyPos, keyClasses);
		
		PactRecordCollector collector = new PactRecordCollector(output);
		// run stub implementation
		while (iter.nextKey())
		{
			stub.reduce(iter.getValues(), collector);
		}
		
		sorter.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void cleanup() throws Exception {
	}
	
}
