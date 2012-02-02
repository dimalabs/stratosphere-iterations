package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class InitialStateComponents extends AbstractMinimalTask {

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		PactRecord record = new PactRecord();
		PactLong initialId = new PactLong();
		
		while (inputs[0].next(record))
		{	
			initialId = record.getField(0, initialId);
			
			record.setField(2, initialId);
			
			output.collect(record);
		}
	}

}
