package eu.stratosphere.pact.iterative.nephele.tasks.util;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.PartitionFunction;

public class IntegerHashPartitioner implements PartitionFunction {
	
	@Override
	public int[] selectChannels(PactRecord data, int numChannels) {
		return new int[] { data.getField(0, PactInteger.class).getValue() % numChannels };
	}

}
