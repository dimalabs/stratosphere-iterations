package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FixedSizeOutputView extends AbstractPagedOutputViewV2 implements SeekableDataOutputView
{
	private final MemorySegment[] segments;
	
	private int currentSegmentIndex;
	
	/**
	 * @param segmentSize
	 * @param headerLength
	 */
	public FixedSizeOutputView(MemorySegment[] segments, int segmentSize, int headerLength)
	{
		super(segments[0], segmentSize, headerLength);
		
		this.segments = segments;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException
	{
		if (++this.currentSegmentIndex < this.segments.length) {
			return this.segments[this.currentSegmentIndex];
		} else {
			throw new EOFException();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView#setWritePosition(long)
	 */
	@Override
	public void setWritePosition(long position)
	{
		final int bufferNum = (int) (position >>> 32);
		final int offset = (int) position;
		
		this.currentSegmentIndex = bufferNum;
		final MemorySegment seg = this.segments[bufferNum];
		
		seekOutput(seg, offset);
	}
}
