package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;

public class BuildCachingMatch extends AbstractIterativeTask {
	
	private SpillingResettableMutableObjectIterator buildIter;
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
		buildIter = new SpillingResettableMutableObjectIterator(
				memoryManager, ioManager, inputs[1],
				memorySize, this);
		buildIter.open();
	}
	
	@Override
	public void invokeIter(IterationIterator probeIter) throws Exception {
		buildIter.reset();
		
		MatchTaskIterator joinIter = new BuildSecondHashMatchIterator(probeIter, 
				buildIter, buildKeyPos, probeKeyPos, keyClasses, 
				memoryManager, ioManager, this, memorySize);
		
		joinIter.open();
		
		while(joinIter.callWithNextKey(stub, output));
		
		joinIter.close();
	}
	
	@Override
	public void cleanup() throws Exception {
		buildIter.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

}
