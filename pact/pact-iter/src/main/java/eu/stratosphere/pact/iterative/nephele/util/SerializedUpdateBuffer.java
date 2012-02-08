package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2;
import eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2;
import eu.stratosphere.pact.runtime.io.MemorySegmentSource;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedUpdateBuffer
{
	private static final int HEADER_LENGTH = 4;
	
	private final Queue<MemorySegment> emptyBuffers;
	
	private final Queue<MemorySegment> internalFullBuffers;
	
	private final Queue<MemorySegment> readBuffers;
	
	private final int segmentSize;
	
	private WriteEnd writeEnd;
	
	private ReadEnd readEnd;
	
	private volatile boolean closed;
	
	
	public SerializedUpdateBuffer() {
		emptyBuffers = null;
		internalFullBuffers = null;
		readBuffers = null;
		segmentSize = -1;
	}
	
	public SerializedUpdateBuffer(List<MemorySegment> memSegments, int segmentSize)
	{
		final MemorySegmentSource emptyBufferSource;
		
		this.segmentSize = segmentSize;
		
		this.emptyBuffers = new ArrayDeque<MemorySegment>();
		this.internalFullBuffers = new ArrayDeque<MemorySegment>();
		this.readBuffers = new ArrayDeque<MemorySegment>();
		
		this.emptyBuffers.addAll(memSegments);
		
		emptyBufferSource = getNonBlockingSourceError(this.emptyBuffers);
		
		// views may only be instantiated after the memory has been loaded
		this.writeEnd = new WriteEnd(this.internalFullBuffers, emptyBufferSource, segmentSize);
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
	
	public void switchBuffers() {
		if(!closed) {
			throw new RuntimeException("Buffer has to be closed before switch");
		}
		
		readBuffers.addAll(internalFullBuffers);
		internalFullBuffers.clear();
		
		final MemorySegmentSource readBufferSource;
		final MemorySegmentSource emptyBufferSource;
		
		emptyBufferSource = getNonBlockingSourceError(this.emptyBuffers);
		readBufferSource = getNonBlockingSource(this.readBuffers);
		
		this.readEnd = new ReadEnd(this.emptyBuffers, readBufferSource, segmentSize);
		this.writeEnd = new WriteEnd(this.internalFullBuffers, emptyBufferSource, segmentSize);
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
	
	private MemorySegmentSource getNonBlockingSourceError(final Queue<MemorySegment> source)
	{
		return new MemorySegmentSource() {
			@Override
			public MemorySegment nextSegment() {
				MemorySegment seg = source.poll();
				if(seg == null) {
					throw new RuntimeException("No more segments");
				}
				
				return seg;
			}
		};
	}
	
	// ============================================================================================
	
	protected static final class WriteEnd extends AbstractPagedOutputViewV2
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
	
	protected static final class ReadEnd extends AbstractPagedInputViewV2
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