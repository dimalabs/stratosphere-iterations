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
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.MemorySegmentSource;


/**
 *
 * The list with the full segments contains at any point all completely full segments, plus the segment that is
 * currently filled.
 *
 * @author Stephan Ewen
 */
public class SimpleCollectingOutputView extends AbstractPagedOutputView
{	
	private final List<MemorySegment> fullSegments;
	
	private final MemorySegmentSource memorySource;


	public SimpleCollectingOutputView(List<MemorySegment> fullSegmentTarget, 
									MemorySegmentSource memSource, int segmentSize)
	{
		super(memSource.nextSegment(), segmentSize, 0);
		this.fullSegments = fullSegmentTarget;
		this.memorySource = memSource;
		this.fullSegments.add(getCurrentSegment());
	}
	
	
	public void reset()
	{
		if (this.fullSegments.size() != 0) {
			throw new IllegalStateException("The target list still contains memory segments.");
		}
	
		clear();
		try {
			advance();
		} catch (IOException ioex) {
			throw new RuntimeException("Error getting first segment for record collector.", ioex);
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException
	{	
		final MemorySegment next = this.memorySource.nextSegment();
		if (next != null) {
			this.fullSegments.add(next);
			return next;
		} else {
			throw new EOFException();
		}
	}
}
