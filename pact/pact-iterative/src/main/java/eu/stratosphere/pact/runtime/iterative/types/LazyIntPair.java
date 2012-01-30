package eu.stratosphere.pact.runtime.iterative.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;
import eu.stratosphere.pact.runtime.iterative.LazyDeSerializable;


/**
 * 
 */
public class LazyIntPair implements LazyDeSerializable
{
	private SeekableDataInputView inView;
	private SeekableDataOutputView outView;
	
	private long position;
	
	
	public LazyIntPair()
	{}
	
	
	public int getKey()
	{
		try {
			this.inView.setReadPosition(this.position);
			return this.inView.readInt();
		} catch (IOException ioex) {
			throw new RuntimeException("Error deserializing key.", ioex);
		}
	}
	
	public void setKey(int key) {
		try {
			this.outView.setWritePosition(this.position);
			this.outView.writeInt(key);
		} catch (IOException ioex) {
			throw new RuntimeException("Error serializing key.", ioex);
		}
	}
	
	public int getValue() {
		try {
			this.inView.setReadPosition(this.position + 4);
			return this.inView.readInt();
		} catch (IOException ioex) {
			throw new RuntimeException("Error deserializing value.", ioex);
		}
	}
	
	public void setValue(int value)
	{
		try {
			this.outView.setWritePosition(this.position + 4);
			this.outView.writeInt(value);
		} catch (IOException ioex) {
			throw new RuntimeException("Error serializing value.", ioex);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.iterative.LazyDerserializable#setDeserializer(eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView, eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView, long)
	 */
	@Override
	public void setDeserializer(SeekableDataInputView inView, SeekableDataOutputView outView, long startPosition) {
		this.inView = inView;
		this.outView = outView;
		this.position = startPosition;
	}
}
