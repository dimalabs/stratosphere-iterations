package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;
import eu.stratosphere.pact.runtime.util.MathUtils;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HeaderlessFixedSizeOutputView extends AbstractPagedOutputViewV2 implements SeekableDataOutputView
{
	private final MemorySegment[] segments;
	
	private int currentSegmentIndex;
	
	private final int segmentSizeBits;
	
	/**
	 * @param segmentSize
	 * @param headerLength
	 */
	public HeaderlessFixedSizeOutputView(MemorySegment[] segments, int segmentSize)
	{
		super(segments[0], segmentSize, 0);
		
		if ((segmentSize & (segmentSize - 1)) != 0)
			throw new IllegalArgumentException("Segment size must be a power of 2!");
		
		this.segments = segments;
		this.segmentSizeBits = MathUtils.log2floor(segmentSize);
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
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & (this.segmentSize - 1));
		
		this.currentSegmentIndex = bufferNum;
		seekOutput(this.segments[bufferNum], offset);
	}
}
