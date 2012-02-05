package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedMutableObjectIteratorV2;

public abstract class PreSortedReduce<T> extends AbstractIterativeTask {

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
		initEnvironmentManagers();
	}
	
	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		KeyGroupedMutableObjectIteratorV2<? extends Value> iter = 
				new KeyGroupedMutableObjectIteratorV2(iterationIter, accessor);
		
		// run stub implementation
		while (iter.nextKey())
		{
			stub.reduce(iter.getValues(), output);
		}
	}
	
	public abstract void reduce(Iterator<PactRecord> records, Collector out) throws Exception;

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void cleanup() throws Exception {
	}
	
}
