package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public class PreSortedReduce extends AbstractIterativeTask {

	private int[] keyPos;
	private Class<? extends Key>[] keyClasses;
	
	private ReduceStub stub;
	
	@Override
	protected void initTask() {
		keyPos = config.getLocalStrategyKeyPositions(0);
		keyClasses =  loadKeyClasses();
		
		stub = initStub(ReduceStub.class);
	}
	
	@Override
	public void invokeStart() throws Exception {
		initEnvManagers();
	}
	
	@Override
	public void invokeIter(IterationIterator iterationIter) throws Exception {
		KeyGroupedIterator iter = new KeyGroupedIterator(iterationIter, keyPos, keyClasses);
		
		// run stub implementation
		while (iter.nextKey())
		{
			stub.reduce(iter.getValues(), output);
		}
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void cleanup() throws Exception {
	}
	
}
