package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class AbstractIterativeMinimalTask extends AbstractMinimalTask {
	
	
	protected MutableObjectIterator<PactRecord> progressInput;
	protected OutputCollector progressOutput;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void initInternal() {
		//Init progress monitor input
		int numInputs = getNumberOfInputs();
		
		final ShipStrategy shipStrategy = config.getInputShipStrategy(numInputs);
		
		if(shipStrategy != ShipStrategy.FORWARD) {
			throw new RuntimeException("Illegal shipstrategy for progress channel" + shipStrategy);
		}
		DistributionPattern dp = new PointwiseDistributionPattern();
		
		this.progressInput = (MutableObjectIterator) 
				new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, dp));
		
		//Init progress monitor output
		progressOutput = new OutputCollector();
		final OutputEmitter oe = new OutputEmitter(shipStrategy);
		final RecordWriter<PactRecord> writer = new RecordWriter<PactRecord>(this, PactRecord.class, oe);
		progressOutput.addWriter(writer);
	}

	@Override
	protected void initTask() {
		// TODO Auto-generated method stub

	}

	@Override
	public int getNumberOfInputs() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void invoke() throws Exception {
		PactRecord progress = new PactRecord();
		
		//Only returns false if channel is closed!
		while(progressInput.next(target)) {
			//If openend ...
			if(progress.getField(0, PactInteger.class).getValue() == 0) {
				
			}
		}
		// TODO Auto-generated method stub

	}

	public void invokeIter() {
		
	}
}
