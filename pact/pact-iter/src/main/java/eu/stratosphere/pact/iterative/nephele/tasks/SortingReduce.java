package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.Comparator;

import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
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
	public void invokeStart() throws Exception {
		initEnvManagers();
	}
	
	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		publishState(ChannelState.OPEN, getEnvironment().getOutputGate(1));
		
		try {
			sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 64, comparators, 
					keyPos, keyClasses, iterationIter, this, 0.8f);
		} catch (Exception ex) {
			throw new RuntimeException("Error creating sorter", ex);
		}
		
		KeyGroupedIterator iter = new KeyGroupedIterator(sorter.getIterator(), keyPos, keyClasses);
		
		// run stub implementation
		while (iter.nextKey())
		{
			stub.reduce(iter.getValues(), output);
		}
		
		sorter.close();
		
		publishState(ChannelState.CLOSED, getEnvironment().getOutputGate(1));
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	protected void initOutputs() {
		// TODO Auto-generated method stub
		super.initOutputs();
	}

}
