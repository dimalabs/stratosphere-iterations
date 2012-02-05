package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessorsV2;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIteratorV2;

public class ProbeCachingMatch extends AbstractIterativeTask {
	
	private SpillingResettableMutableObjectIteratorV2<? extends Value> probeIter;
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
		probeIter = new SpillingResettableMutableObjectIteratorV2(
				memoryManager, ioManager, inputs[1], accessor,
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
