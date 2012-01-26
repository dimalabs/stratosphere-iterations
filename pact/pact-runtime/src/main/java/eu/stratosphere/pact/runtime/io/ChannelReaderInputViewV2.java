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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A {@link DataInputView} that is backed by a {@link BlockChannelReader}, making it effectively a data input
 * stream. The view reads it data in blocks from the underlying channel. The view can only read data that
 * has been written by a {@link ChannelWriterOutputView}, due to block formatting.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ChannelReaderInputViewV2 extends AbstractPagedInputViewV2
{
	private final BlockChannelReader reader;
	
	private final int numSegments;
	
	private int numRequestsRemaining;
	
	private long positionBeforeSegment;
	
	private boolean inLastBlock;
	
	private boolean closed;
	
	// --------------------------------------------------------------------------------------------
	
	
	public ChannelReaderInputViewV2(BlockChannelReader reader, List<MemorySegment> memory, boolean waitForFirstBlock)
	throws IOException
	{
		this(reader, memory, -1, waitForFirstBlock);
	}
	
	public ChannelReaderInputViewV2(BlockChannelReader reader, List<MemorySegment> memory, 
														int numBlocks, boolean waitForFirstBlock)
	throws IOException
	{
		super(ChannelWriterOutputViewV2.HEADER_LENGTH);
		
		if (reader == null || memory == null)
			throw new NullPointerException();
		if (memory.isEmpty())
			throw new IllegalArgumentException("Empty list of memory segments given.");
		if (numBlocks < 1 && numBlocks != -1) {
			throw new IllegalArgumentException("The number of blocks must be a positive number, or -1, if unknown.");
		}
		
		this.reader = reader;
		this.numRequestsRemaining = numBlocks;
		this.numSegments = memory.size();
		
		for (int i = 0; i < memory.size(); i++) {
			sendReadRequest(memory.get(i));
		}
		
		if (waitForFirstBlock) {
			advance();
		}
	}
	
	public void waitForFirstBlock() throws IOException
	{
		if (getCurrentSegment() == null) {
			advance();
		}
	}
	
	/**
	 * Closes this InoutView, closing the underlying reader and returning all memory segments.
	 * 
	 * @return A list containing all memory segments originally supplied to this view.
	 * @throws IOException Thrown, if the underlying reader could not be properly closed.
	 */
	public List<MemorySegment> close() throws IOException
	{	
		if (this.closed) {
			throw new IllegalStateException("Already closed.");
		}
		this.closed = true;
		
		// re-collect all memory segments
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(this.numSegments);
		final MemorySegment current = getCurrentSegment();
		if (current != null) {
			list.add(current);
		}
		clear();

		// close the writer and gather all segments
		final LinkedBlockingQueue<MemorySegment> queue = this.reader.getReturnQueue();
		this.reader.close();

		for (int i = 1; i < this.numSegments; i++) {
			final MemorySegment m = queue.poll();
			if (m == null) {
				// we get null if the queue is empty. that should not be the case if the reader was properly closed.
				throw new RuntimeException("Bug in ChannelReaderInputView: MemorySegments lost.");
			}
			list.add(m);
		}
		return list;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Utilities
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException
	{
		// check if we are at our end
		if (this.inLastBlock) {
			throw new EOFException();
		}
				
		// send a request first. if we have only a single segment, this same segment will be the one obtained in
		// the next lines
		if (current != null) {
			this.positionBeforeSegment += getCurrentSegmentLimit() - ChannelWriterOutputView.HEADER_LENGTH;
			sendReadRequest(current);
		}
		
		// get the next segment
		final MemorySegment seg;
		try {
			seg = this.reader.getReturnQueue().take();
		}
		catch (InterruptedException iex) {
			throw new IOException("ChannelWriterOutputView was interrupted while waiting for the next buffer");
		}
		
		// check the header
		if (seg.getShort(0) != ChannelWriterOutputView.HEADER_MAGIC_NUMBER) {
			throw new IOException("The current block does not belong to a ChannelWriterOutputView / ChannelReaderInputView: Wrong magic number.");
		}
		if ( (seg.getShort(ChannelWriterOutputView.HEADER_FLAGS_OFFSET) & ChannelWriterOutputView.FLAG_LAST_BLOCK) != 0) {
			// last block
			this.numRequestsRemaining = 0;
			this.inLastBlock = true;
		}
		
		return seg;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected int getLimitForSegment(MemorySegment segment) throws IOException
	{
		return segment.getInt(ChannelWriterOutputView.HEAD_BLOCK_LENGTH_OFFSET);
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException
	{
		if (this.numRequestsRemaining != 0) {
			this.reader.readBlock(seg);
			if (this.numRequestsRemaining != -1) {
				this.numRequestsRemaining--;
			}
		} else {
			// directly add it to the end of the return queue
			this.reader.getReturnQueue().add(seg);
		}
	}
}
