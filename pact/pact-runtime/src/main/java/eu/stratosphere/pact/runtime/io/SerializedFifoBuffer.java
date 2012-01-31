package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedFifoBuffer
{
	private static final int HEADER_LENGTH = 4;
	
	private final Queue<MemorySegment> emptyBuffers;
	
	private final Queue<MemorySegment> fullBuffers;
	
	private final WriteEnd writeEnd;
	
	private final ReadEnd readEnd;
	
	private volatile boolean closed;
	
	
	
	public SerializedFifoBuffer(List<MemorySegment> memSegments, int segmentSize, boolean blocking)
	{
		final MemorySegmentSource fullBufferSource;
		final MemorySegmentSource emptyBufferSource;
		
		if (blocking) {
			this.emptyBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size());
			this.fullBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size());
			
			fullBufferSource = getBlockingSource((ArrayBlockingQueue<MemorySegment>) this.emptyBuffers);
			emptyBufferSource = getBlockingSource((ArrayBlockingQueue<MemorySegment>) this.fullBuffers);
		} else {
			this.emptyBuffers = new ArrayDeque<MemorySegment>();
			this.fullBuffers = new ArrayDeque<MemorySegment>();
			
			emptyBufferSource = getNonBlockingSource(this.emptyBuffers);
			fullBufferSource = getNonBlockingSource(this.fullBuffers);
		}
		
		// load the memory
		for (int i = memSegments.size() - 1; i >= 0; --i) {
			this.emptyBuffers.add(memSegments.remove(i));
		}
		
		// views may only be instantiated after the memory has been loaded
		this.readEnd = new ReadEnd(this.emptyBuffers, fullBufferSource, segmentSize);
		this.writeEnd = new WriteEnd(this.fullBuffers, emptyBufferSource, segmentSize);
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
				if (SerializedFifoBuffer.this.closed && source.isEmpty()) {
					return null;
				} else {
					try {
						return source.take();
					} catch (InterruptedException e) {
						throw new RuntimeException("FifoBuffer was interrupted waiting next memory segment.");
					}
				}
			}
		};
	}
	
	private MemorySegmentSource getNonBlockingSource(final Queue<MemorySegment> source)
	{
		return new MemorySegmentSource() {
			@Override
			public MemorySegment nextSegment() {
				return source.poll();
			}
		};
	}
	
	// ============================================================================================
	
	private static final class WriteEnd extends AbstractPagedOutputViewV2
	{	
		private final Queue<MemorySegment> fullBufferTarget;
		
		private final MemorySegmentSource emptyBufferSource;
		
		WriteEnd(Queue<MemorySegment> fullBufferTarget, MemorySegmentSource emptyBufferSource, int segmentSize)
		{
			super(emptyBufferSource.nextSegment(), segmentSize, HEADER_LENGTH);
			
			this.fullBufferTarget = fullBufferTarget;
			this.emptyBufferSource = emptyBufferSource;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
		 */
		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException
		{
			current.putInt(0, positionInCurrent);
			this.fullBufferTarget.add(current);	
			return this.emptyBufferSource.nextSegment();
		}
		
		void flush() throws IOException
		{
			advance();
		}
	}
	
	private static final class ReadEnd extends AbstractPagedInputViewV2
	{
		private final Queue<MemorySegment> emptyBufferTarget;
		
		private final MemorySegmentSource fullBufferSource;
		
		ReadEnd(Queue<MemorySegment> emptyBufferTarget, MemorySegmentSource fullBufferSource, int segmentSize)
		{
			super(HEADER_LENGTH);
			
			this.emptyBufferTarget = emptyBufferTarget;
			this.fullBufferSource = fullBufferSource;
			
			seekInput(emptyBufferTarget.poll(), segmentSize, segmentSize);
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
		 */
		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws IOException {
			this.emptyBufferTarget.add(current);
			
			final MemorySegment seg = this.fullBufferSource.nextSegment();
			if (seg != null) {
				return seg;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
		 */
		@Override
		protected int getLimitForSegment(MemorySegment segment) throws IOException {
			return segment.getInt(0);
		}
	}
}