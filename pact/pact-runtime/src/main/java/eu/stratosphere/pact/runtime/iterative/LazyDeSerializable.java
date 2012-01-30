package eu.stratosphere.pact.runtime.iterative;

import eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface LazyDeSerializable
{
	public void setDeserializer(SeekableDataInputView inView, SeekableDataOutputView outView, long startPosition);
}
