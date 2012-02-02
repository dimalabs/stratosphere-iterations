package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Iterator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.io.SerializedFifoBuffer;

public class ModernSerializingQueue extends AbstractQueue<PactRecord> {
	
	private static final int DEFAULT_MEMORY_SEGMENT_SIZE = 8 * 1024 * 1024;
	
	private final SerializedFifoBuffer buffer;
	private final DataInputViewV2 readView;
	private final DataOutputViewV2 writeView;
	private final PactRecord currentReadRecord;
	
	private int count;
	private boolean readNext;
	
	public ModernSerializingQueue(final AbstractInvokable task) {
		//memoryManager = task.getEnvironment().getMemoryManager();
		//TODO: make it work with serialized fifo buffer
		buffer = null;
		//buffer = new SerializedFifoBuffer(memSegments, DEFAULT_MEMORY_SEGMENT_SIZE, false);
		readView = buffer.getReadEnd();
		writeView = buffer.getWriteEnd();
		currentReadRecord = new PactRecord();
	}
	
	@Override
	public boolean offer(PactRecord rec) {
		try {
			rec.write(writeView);
		} catch (IOException ex) {
			throw new RuntimeException("Could not serialize record into queue", ex);
		}
		
		count++;
		
		return true;
	}

	@Override
	public PactRecord peek() {
		if(readNext) {
			try {
				currentReadRecord.read(readView);
			} catch (IOException ex) {
				throw new RuntimeException("Could not deserialize record from queue", ex);
			}
			
			readNext = false;
		}
		
		return currentReadRecord;
	}

	@Override
	public PactRecord poll() {
		PactRecord rec = peek();
		readNext = true;
		count--;
		return rec;
	}

	@Override
	public Iterator<PactRecord> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return count;
	}
	
	@Override
	public void clear() {
		buffer.close();
		
		count = 0;
	}
}
