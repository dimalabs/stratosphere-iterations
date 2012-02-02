/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.io;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A {@link DataOutputView} that is backed by a {@link BlockChannelWriter}, making it effectively a data output
 * stream. The view writes it data in blocks to the underlying channel, adding a minimal header to each block.
 * The data can be re-read by a {@link ChannelReaderInputView}, if it uses the same block size.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class ChannelWriterOutputView extends AbstractPagedOutputView
{
	/**
	 * The magic number that identifies blocks as blocks from a ChannelWriterOutputView.
	 */
	protected static final short HEADER_MAGIC_NUMBER = (short) 0xC0FE;
	
	/**
	 * The length of the header put into the blocks.
	 */
	protected static final int HEADER_LENGTH = 8;
	
	/**
	 * The offset to the flags in the header;
	 */
	protected static final int HEADER_FLAGS_OFFSET = 2;
	
	/**
	 * The offset to the header field indicating the number of bytes in the block
	 */
	protected static final int HEAD_BLOCK_LENGTH_OFFSET = 4;
	
	/**
	 * The flag marking a block as the last block.
	 */
	protected static final short FLAG_LAST_BLOCK = (short) 0x1;
	
	// --------------------------------------------------------------------------------------------
	
	private final BlockChannelWriter writer;		// the writer to the channel
	
	private long bytesBeforeSegment;				// the number of bytes written before the current memory segment
	
	private int blockCount;							// the number of blocks used
	
	private final int numSegments;					// the number of memory segments used by this view
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates an new <code>ChannelWriterOutputView</code> that writes to the given channels and buffers data
	 * in the given memory segments.
	 * 
	 * @param writer The writer to write to.
	 * @param memory The memory used to buffer data.
	 * @param segmentSize The size of the memory segments.
	 */
	public ChannelWriterOutputView(BlockChannelWriter writer, List<MemorySegment> memory, int segmentSize)
	{
		
		super(segmentSize, HEADER_LENGTH);
		
		if (writer == null || memory == null)
			throw new NullPointerException();
		if (memory.isEmpty())
			throw new IllegalArgumentException("Empty memory segment collection given.");
		
		this.writer = writer;
		this.numSegments = memory.size();
		
		// load the segments into the queue
		final LinkedBlockingQueue<MemorySegment> queue = writer.getReturnQueue();
		for (int i = memory.size() - 1; i >= 0; --i) {
			final MemorySegment seg = memory.get(i);
			if (seg.size() != segmentSize) {
				throw new IllegalArgumentException("The supplied memory segments are not of the specified size.");
			}
			queue.add(seg);
		}
		
		// get the first segment
		try {
			advance();
		}
		catch (IOException ioex) {
			throw new RuntimeException("BUG: IOException occurred while getting first block for ChannelWriterOutputView.", ioex);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Closes this OutputView, closing the underlying writer and returning all memory segments.
	 * 
	 * @return A list containing all memory segments originally supplied to this view.
	 * @throws IOException Thrown, if the underlying writer could not be properly closed.
	 */
	public List<MemorySegment> close() throws IOException
	{
		// send off set last segment
		writeSegment(getCurrentSegment(), getCurrentPositionInSegment(), true);
		clear();
		
		// close the writer and gather all segments
		final LinkedBlockingQueue<MemorySegment> queue = this.writer.getReturnQueue();
		this.writer.close();
		
		// re-collect all memory segments
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(this.numSegments);	
		for (int i = 0; i < this.numSegments; i++) {
			final MemorySegment m = queue.poll();
			if (m == null) {
				// we get null if the queue is empty. that should not be the case if the reader was properly closed.
				throw new RuntimeException("Bug in ChannelWriterOutputView: MemorySegments lost.");
			}
			list.add(m);
		}
		
		return list;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of blocks used by this view.
	 * 
	 * @return The number of blocks used.
	 */
	public int getBlockCount()
	{
		return this.blockCount;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#getPosition()
	 */
	@Override
	public int getPosition() {
		throw new UnsupportedOperationException();
//		if (this.positionBeforeSegment + this.positionInSegment <= Integer.MAX_VALUE) {
//			return (int) (this.positionBeforeSegment + this.positionInSegment);
//		}
//		else {
//			throw new RuntimeException("ChannelWriterOutput View exceeded int addressable size - Still incompatible with current memory layout.");
//		}
	}
	
	public long getBytesWritten() {
		return this.bytesBeforeSegment + getCurrentPositionInSegment() - HEADER_LENGTH;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#setPosition(int)
	 */
	@Override
	public DataOutput setPosition(int position) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#reset()
	 */
	@Override
	public DataOutputView reset() {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#getRemainingBytes()
	 */
	@Override
	public int getRemainingBytes() {
		// the number of remaining bytes is infinite
		return -1;
	}

	// --------------------------------------------------------------------------------------------
	//                                      Page Management
	// --------------------------------------------------------------------------------------------
	
	protected final MemorySegment nextSegment(MemorySegment current) throws IOException
	{
		if (current != null) {
			writeSegment(current, current.size(), false);
		}
		
		try {
			final MemorySegment next = this.writer.getReturnQueue().take();
			this.blockCount++;
			return next;
		}
		catch (InterruptedException iex) {
			throw new IOException("ChannelWriterOutputView was interrupted while waiting for the next buffer");
		}
	}
	
	private final void writeSegment(MemorySegment segment, int writePosition, boolean lastSegment) throws IOException
	{
		segment.putShort(0, HEADER_MAGIC_NUMBER);
		segment.putShort(HEADER_FLAGS_OFFSET, lastSegment ? FLAG_LAST_BLOCK : 0);
		segment.putInt(HEAD_BLOCK_LENGTH_OFFSET, writePosition);
		
		this.writer.writeBlock(segment);
		this.bytesBeforeSegment += writePosition - HEADER_LENGTH;
	}
}
