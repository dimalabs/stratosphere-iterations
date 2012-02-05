package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

public class DeserializingIterator implements MutableObjectIterator<Value> {

	private DataInputViewV2 input;
	
	public DeserializingIterator(DataInputViewV2 input) {
		this.input = input;
	}
	@Override
	public boolean next(Value target) throws IOException {
		try {
			target.read(input);
		} catch (EOFException ex) {
			return false;
		}
		
		return true;
	}

}
