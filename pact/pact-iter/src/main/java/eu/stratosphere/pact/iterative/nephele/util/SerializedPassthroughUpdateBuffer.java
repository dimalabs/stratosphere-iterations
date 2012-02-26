package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.runtime.io.MemorySegmentSource;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedPassthroughUpdateBuffer extends SerializedUpdateBufferOld
{	
	private final ArrayBlockingQueue<MemorySegment> emptyBuffers;
	
	private final ArrayBlockingQueue<MemorySegment> fullBuffers;
	
	private WriteEnd writeEnd;
	
	private ReadEnd readEnd;
	
	private volatile boolean closed;
	
	private final java.util.concurrent.atomic.AtomicInteger count;
	private final Lock lock;
	
	
	public SerializedPassthroughUpdateBuffer(List<MemorySegment> memSegments, int segmentSize)
	{
		count = new AtomicInteger();
		lock = new ReentrantLock();
		final MemorySegmentSource fullBufferSource;
		final MemorySegmentSource emptyBufferSource;
		
		this.emptyBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size()+1);
		this.fullBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size()+1);
		
		fullBufferSource = getTimeoutSource((ArrayBlockingQueue<MemorySegment>) this.fullBuffers);
		emptyBufferSource = getBlockingSource((ArrayBlockingQueue<MemorySegment>) this.emptyBuffers);
		
		this.emptyBuffers.addAll(memSegments);
		
		// views may only be instantiated after the memory has been loaded
		this.readEnd = new ReadEnd(this.emptyBuffers, fullBufferSource, segmentSize);
		this.writeEnd = new WriteEnd(this.fullBuffers, emptyBufferSource, segmentSize);
	}
	
	public void incCount() {
		count.incrementAndGet();
	}
	
	public void decCount() {
		count.decrementAndGet();
	}
	
	public int getCount() {
		return count.get();
	}
	
	public void lock() {
		lock.lock();
	}
	
	public void unlock() {
		lock.unlock();
	}
	
	public WriteEnd getWriteEnd() {
		return writeEnd;
	}
	
	public ReadEnd getReadEnd() {
		return readEnd;
	}
	
	public void flush() throws IOException {
		this.writeEnd.flush();
	}
	
	public void close() {
		this.closed = true;
	}
	
	private MemorySegmentSource getBlockingSource(final ArrayBlockingQueue<MemorySegment> source)
	{
		return new MemorySegmentSource() {
			@Override
			public MemorySegment nextSegment() {
				if (SerializedPassthroughUpdateBuffer.this.closed && source.isEmpty()) {
					return null;
				} else {
					try {
						return source.take();
					} catch (InterruptedException e) {
						throw new RuntimeException("Interrupted!!", e);
					}
				}
			}
		};
	}
	
	private volatile boolean blocks = false;
	
	private MemorySegmentSource getTimeoutSource(final ArrayBlockingQueue<MemorySegment> source)
	{
		return new MemorySegmentSource() {
			int count = 0;
			int count2 = 0;
			@Override
			public MemorySegment nextSegment() {
				if (SerializedPassthroughUpdateBuffer.this.closed && source.isEmpty()) {
					return null;
				} else {
					try {
						MemorySegment seg;
						if(count < 2) {
							seg = source.take();
						} else {
							seg = source.poll();
						}
						while(seg == null) {
							if(SerializedPassthroughUpdateBuffer.this.getCount() > 0) {
								SerializedPassthroughUpdateBuffer.this.lock();
								SerializedPassthroughUpdateBuffer.this.writeEnd.flush();
								SerializedPassthroughUpdateBuffer.this.unlock();
								seg = source.take();
								count2 = 0;
							} else if(count2 < 100) {
								blocks = true;
								seg = source.poll(5, TimeUnit.MILLISECONDS);
								count2++;
							} else {
								return null;
							}
						}
						
						count++;
						count2 = 0;
						return seg;
					} catch (InterruptedException e) {
						throw new RuntimeException("FifoBuffer was interrupted waiting next memory segment.");
					} catch (IOException e) {
						throw new RuntimeException("Boooo");
					}
				}
			}
		};
	}

	public boolean isBlocking() {
		return blocks;
	}
}