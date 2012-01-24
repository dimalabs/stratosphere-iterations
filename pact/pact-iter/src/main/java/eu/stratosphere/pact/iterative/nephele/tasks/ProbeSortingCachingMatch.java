package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.Comparator;

import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.KeyComparator;

public class ProbeSortingCachingMatch extends AbstractIterativeTask {
	
	private SpillingResettableMutableObjectIterator probeIter;
	private int[] buildKeyPos;
	private int[] probeKeyPos;
	private Class<? extends Key>[] keyClasses;
	
	private MatchStub stub;
	
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
		probeIter = new SpillingResettableMutableObjectIterator(
				memoryManager, ioManager, inputs[1],
				memorySize, this);
		probeIter.open();
	}
	
	@Override
	public void invokeIter(IterationIterator buildIter) throws Exception {
		probeIter.reset();
		
		BuildFirstHashMatchIterator joinIter = new BuildFirstHashMatchIterator(buildIter, 
				probeIter, buildKeyPos, probeKeyPos, keyClasses, 
				memoryManager, ioManager, this, memorySize);
		
		joinIter.open();
		
		while(joinIter.callWithNextKey(stub, output));
		
		joinIter.close();
	}
	
	@Override
	public void cleanup() throws Exception {
		probeIter.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

}
