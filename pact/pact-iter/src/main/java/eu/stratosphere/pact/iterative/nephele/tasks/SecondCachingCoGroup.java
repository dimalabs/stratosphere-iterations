package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.SortMergeCoGroupIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class SecondCachingCoGroup extends AbstractIterativeTask {
	
	private SpillingResettableMutableObjectIterator iter2;
	private int[] keyPos1;
	private int[] keyPos2;
	private Class<? extends Key>[] keyClasses;
	
	private CoGroupStub stub;
	
	@Override
	protected void initTask() {
		keyPos1 = config.getLocalStrategyKeyPositions(0);
		keyPos2 = config.getLocalStrategyKeyPositions(1);
		keyClasses =  loadKeyClasses();
		
		stub = initStub(CoGroupStub.class);
	}
	
	@Override
	public void invokeStart() throws Exception {
		initEnvManagers();
		iter2 = new SpillingResettableMutableObjectIterator(
				memoryManager, ioManager, inputs[1],
				memorySize, this);
		iter2.open();
	}
	
	@Override
	public void invokeIter(IterationIterator iter1) throws Exception {
		iter2.reset();
		
		SortMergeCoGroupIterator coGroupIter = new SortMergeCoGroupIterator(memoryManager, ioManager, 
				iter1, iter2, keyPos1, keyPos2, keyClasses, 
				memorySize, 64, 0.9f, LocalStrategy.SORT_FIRST_MERGE, this);
		
		coGroupIter.open();
		
		while (coGroupIter.next()) {
			stub.coGroup(coGroupIter.getValues1(), coGroupIter.getValues2(), 
					output);
		}
		
		coGroupIter.close();
	}
	
	@Override
	public void cleanup() throws Exception {
		iter2.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

}
