package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

public class DeserializingIterator implements MutableObjectIterator<Value> {

	protected static final Log LOG = LogFactory.getLog(DeserializingIterator.class);
	
	private DataInputViewV2 input;
	
	public DeserializingIterator(DataInputViewV2 input) {
		this.input = input;
		LOG.info("Init: Deserializer");
	}
	@Override
	public boolean next(Value target) throws IOException {
		try {
			target.read(input);
		} catch (EOFException ex) {
			LOG.info("Finished: Deserializer");
			return false;
		}
		return true;
	}

}
