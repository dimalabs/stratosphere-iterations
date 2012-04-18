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

package eu.stratosphere.pact.runtime.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.util.PactRecordRepeatableIterator;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that realizes the
 * matching through a sort-merge join strategy.
 *
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class SortMergeMatchIterator implements MatchTaskIterator
{
	/**
	 * The log used by this iterator to log messages.
	 */
	private static final Log LOG = LogFactory.getLog(SortMergeMatchIterator.class);
	
	/**
	 * The fraction of the memory that is dedicated to the spilling resettable iterator, which is used in cases where
	 * the cross product of values with the same key becomes very large. 
	 */
	private static final float DEFAULT_MEMORY_SHARE_RATIO = 0.05f;
	
	// --------------------------------------------------------------------------------------------
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final MutableObjectIterator<PactRecord> reader1;

	private final MutableObjectIterator<PactRecord> reader2;
	
	private final int[] firstKeyPositions;
	
	private final int[] secondKeyPositions;
	
	private final Class<? extends Key>[] keyClasses;
	
	private final PactRecord copy1 = new PactRecord();
	
	private final PactRecord copy2 = new PactRecord();
	
	private final PactRecord instance = new PactRecord();
	
	private final BlockResettableIterator blockIt;				// for N:M cross products with same key
	
	private final LocalStrategy localStrategy;
	
	private final AbstractTask parentTask;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;

	
	private SortMerger sortMerger1;

	private SortMerger sortMerger2;
	
	private KeyGroupedIterator iterator1;

	private KeyGroupedIterator iterator2;

	
	public SortMergeMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<PactRecord> reader1, MutableObjectIterator<PactRecord> reader2,
			int[] firstInputKeyPositions, int[] secondInputKeyPositions, Class<? extends Key>[] keyClasses,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractTask parentTask)
	throws MemoryAllocationException
	{
		this(memoryManager, ioManager, reader1, reader2, 
			firstInputKeyPositions, secondInputKeyPositions, keyClasses,
			memory, maxNumFileHandles, spillingThreshold, DEFAULT_MEMORY_SHARE_RATIO, 
			localStrategy, parentTask);
	}
	
	public SortMergeMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<PactRecord> reader1, MutableObjectIterator<PactRecord> reader2,
			int[] firstInputKeyPositions, int[] secondInputKeyPositions, Class<? extends Key>[] keyClasses,
			long memory, int maxNumFileHandles, float spillingThreshold, float memPercentageForBlockNL,
			LocalStrategy localStrategy, AbstractTask parentTask)
	throws MemoryAllocationException
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.reader1 = reader1;
		this.reader2 = reader2;
		
		this.keyClasses = keyClasses;
		this.firstKeyPositions = firstInputKeyPositions;
		this.secondKeyPositions = secondInputKeyPositions;
		
		long memoryForBlockNestedLoops = Math.max((long) (memory * memPercentageForBlockNL),
			SpillingResettableIterator.MIN_TOTAL_MEMORY + BlockResettableMutableObjectIterator.MIN_BUFFER_SIZE);
		
		this.memoryPerChannel = (memory - memoryForBlockNestedLoops) / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
		
		this.blockIt = new BlockResettableIterator(this.memoryManager,
			memoryForBlockNestedLoops - SpillingResettableIterator.MIN_TOTAL_MEMORY, 1, parentTask);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{
		// comparator
		final Comparator<Key> keyComparator = new KeyComparator();
		
		@SuppressWarnings("unchecked")
		final Comparator<Key>[] comparators = (Comparator<Key>[]) new Comparator[this.keyClasses.length];
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = keyComparator;
		}
			
		// ================================================================
		//                   PERFORMANCE NOTICE
		//
		// It is important to instantiate the sort-mergers both before 
		// obtaining the iterator from one of them. The reason is that
		// the getIterator() method freezes until the first value is
		// available and both sort-mergers should be instantiated and
		// running in the background before this thread waits.
		// ================================================================

		// iterator 1
		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_FIRST_MERGE)
		{
			// merger
			this.sortMerger1 = new UnilateralSortMerger(this.memoryManager, this.ioManager,
					this.memoryPerChannel, this.fileHandlesPerChannel, 
					comparators, this.firstKeyPositions, this.keyClasses, 
					this.reader1, this.parentTask, this.spillingThreshold);
		}

		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			// merger
			this.sortMerger2 = new UnilateralSortMerger(this.memoryManager, this.ioManager, 
					this.memoryPerChannel, this.fileHandlesPerChannel,
					comparators, this.secondKeyPositions, this.keyClasses,
					this.reader2, this.parentTask, this.spillingThreshold);
		}
			
		// =============== These calls freeze until the data is actually available ============ 
		
		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyGroupedIterator(this.sortMerger1.getIterator(), this.firstKeyPositions, this.keyClasses);
				this.iterator2 = new KeyGroupedIterator(this.sortMerger2.getIterator(), this.secondKeyPositions, this.keyClasses);
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyGroupedIterator(this.sortMerger1.getIterator(), this.firstKeyPositions, this.keyClasses);
				this.iterator2 = new KeyGroupedIterator(this.reader2, this.secondKeyPositions, this.keyClasses);
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyGroupedIterator(this.reader1, this.firstKeyPositions, this.keyClasses);
				this.iterator2 = new KeyGroupedIterator(this.sortMerger2.getIterator(), this.secondKeyPositions, this.keyClasses);
				break;
			case MERGE:
				this.iterator1 = new KeyGroupedIterator(this.reader1, this.firstKeyPositions, this.keyClasses);
				this.iterator2 = new KeyGroupedIterator(this.reader2, this.secondKeyPositions, this.keyClasses);
				break;
			default:
				throw new RuntimeException("Unsupported Local Strategy in SortMergeMatchIterator: "+this.localStrategy);
		}
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#close()
	 */
	@Override
	public void close()
	{
		if (this.blockIt != null) {
			try {
				this.blockIt.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing block memory iterator: " + t.getMessage(), t);
			}
		}
		
		// close the two sort/merger to release the memory segments
		if (this.sortMerger1 != null) {
			try {
				this.sortMerger1.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sort/merger for first input: " + t.getMessage(), t);
			}
		}
		
		if (this.sortMerger2 != null) {
			try {
				this.sortMerger2.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sort/merger for second input: " + t.getMessage(), t);
			}
		}
	}
	
	/**
	 * Calls the <code>MatchStub#match()</code> method for all two key-value pairs that share the same key and come 
	 * from different inputs. The output of the <code>match()</code> method is forwarded.
	 * <p>
	 * This method first zig-zags between the two sorted inputs in order to find a common
	 * key, and then calls the match stub with the cross product of the values.
	 * 
	 * @throws Exception Forwards all exceptions from the user code and the I/O system.
	 * 
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#callWithNextKey()
	 */
	@Override
	public boolean callWithNextKey(MatchStub matchFunction, Collector collector)
	throws Exception
	{
		if (!this.iterator1.nextKey() || !this.iterator2.nextKey()) {
			return false;
		}

		Key[] keys1 = this.iterator1.getKeys();
		Key[] keys2 = this.iterator2.getKeys();

		// zig zag
		while (true) {
			// determine the relation between the (possibly composite) keys
			int comp = 0;
			for (int i = 0; i < keys1.length; i++) {
				int c = keys1[i].compareTo(keys2[i]);
				if (c != 0) {
					comp = c;
					break;
				}
			}
			
			if (comp == 0)
				break;
			
			if (comp > 0) {
				if (!this.iterator2.nextKey()) {
					return false;
				}
				keys2 = this.iterator2.getKeys();
			}
			else {
				if (!this.iterator1.nextKey()) {
					return false;
				}
				keys1 = this.iterator1.getKeys();
			}
		}
		
		// here, we have a common key! call the match function with the cross product of the
		// values
		final KeyGroupedIterator.ValuesIterator values1 = this.iterator1.getValues();
		final KeyGroupedIterator.ValuesIterator values2 = this.iterator2.getValues();
		
		final PactRecord firstV1 = values1.next().createCopy();
		final PactRecord firstV2 = values2.next().createCopy();	
		
		if (firstV1 == null || firstV2 == null) {
			return false;
		}
			
		final boolean v1HasNext = values1.hasNext();
		final boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		if (!v1HasNext && !v2HasNext) {
			// both sides contain only one value
			matchFunction.match(firstV1, firstV2, collector);
		}
		else if (!v1HasNext) {
			crossFirst1withNValues(firstV1, firstV2, values2, matchFunction, collector);

		}
		else if (!v2HasNext) {
			crossSecond1withNValues(firstV2, firstV1, values1, matchFunction, collector);
		}
		else {
			// both sides contain more than one value
			// TODO: Decide which side to spill and which to block!
			crossMwithNValues(firstV1, values1, firstV2, values2, matchFunction, collector);
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#abort()
	 */
	@Override
	public void abort()
	{}
	
	// ==============================================================================
	
	/**
	 * Crosses a single value from the first input with N values, all sharing a common key.
	 * Effectively realizes a <i>1:N</i> match (join).
	 * 
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws Exception Forwards all exceptions thrown by the stub.
	 */
	private void crossFirst1withNValues(PactRecord val1, PactRecord firstValN,
			Iterator<PactRecord> valsN, MatchStub matchFunction, Collector collector)
	throws Exception
	{
		val1.copyTo(this.copy1);
		matchFunction.match(val1, firstValN, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			PactRecord nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.copy1.copyToIfModified(val1);
				matchFunction.match(val1, nRec, collector);
			} else {
				matchFunction.match(this.copy1, nRec, collector);
				more = false;
			}
		}
		while (more);
	}
	
	/**
	 * Crosses a single value from the second side with N values, all sharing a common key.
	 * Effectively realizes a <i>N:1</i> match (join).
	 * 
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws Exception Forwards all exceptions thrown by the stub.
	 */
	private void crossSecond1withNValues(PactRecord val1, PactRecord firstValN,
			Iterator<PactRecord> valsN, MatchStub matchFunction, Collector collector)
	throws Exception
	{
		val1.copyTo(this.copy1);
		matchFunction.match(firstValN, val1, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			PactRecord nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.copy1.copyToIfModified(val1);
				matchFunction.match(nRec, val1, collector);
			} else {
				matchFunction.match(nRec, this.copy1, collector);
				more = false;
			}
		}
		while (more);
	}
	
	/**
	 * @param firstV1
	 * @param spillVals
	 * @param firstV2
	 * @param blockVals
	 */
	private void crossMwithNValues(final PactRecord firstV1, Iterator<PactRecord> spillVals,
			final PactRecord firstV2, final Iterator<PactRecord> blockVals,
			MatchStub matchFunction, Collector collector)
	throws Exception
	{
		// ==================================================
		// We have one first (head) element from both inputs (firstV1 and firstV2)
		// We have an iterator for both inputs.
		// we make the V1 side the spilling side and the V2 side the blocking side.
		// In order to get the full cross product without unnecessary spilling, we do the
		// following:
		// 1) cross the heads
		// 2) cross the head of the spilling side against the first block of the blocking side
		// 3) cross the iterator of the spilling side with the head of the block side
		// 4) cross the iterator of the spilling side with the first block
		// ---------------------------------------------------
		// If the blocking side has more than one block, we really need to make the spilling side fully
		// resettable. For each further block on the block side, we do:
		// 5) cross the head of the spilling side with the next block
		// 6) cross the spilling iterator with the next block.
		
		// match the first values first
		firstV1.copyTo(this.copy1);
		firstV2.copyTo(this.copy2);
		
		// --------------- 1) Cross the heads -------------------
		matchFunction.match(firstV1, firstV2, collector);
		
		// for the remaining values, we do a block-nested-loops join
		SpillingResettableIterator<PactRecord> spillIt = null;
		
		try {
			// create block iterator on the second input
			this.blockIt.reopen(blockVals);
			
			// ------------- 2) cross the head of the spilling side with the first block ------------------
			while (blockIt.hasNext()) {
				PactRecord nextBlockRec = blockIt.next();
				this.copy1.copyTo(this.instance);
				matchFunction.match(this.instance, nextBlockRec, collector);
			}
			blockIt.reset();
			
			// spilling is required if the blocked input has data beyond the current block.
			// in that case, create the spilling iterator
			final LastRepeatableIterator<PactRecord> repeatableIter;
			boolean spillingRequired = blockIt.hasFurtherInput();
			if (spillingRequired)
			{
				// more data than would fit into one block. we need to wrap the other side in a spilling iterator
				// create spilling iterator on first input
				spillIt = new SpillingResettableIterator<PactRecord>(this.memoryManager, this.ioManager, spillVals, 
						new PactRecord(), SpillingResettableIterator.MIN_TOTAL_MEMORY, this.parentTask);
				repeatableIter = spillIt;				
				spillIt.open();
			}
			else {
				repeatableIter = new PactRecordRepeatableIterator(spillVals);
			}
			
			// cross the values in the v1 iterator against the current block
			
			while (repeatableIter.hasNext()) {
				PactRecord nextSpillVal = repeatableIter.next();
				
				// -------- 3) cross the iterator of the spilling side with the head of the block side --------
				this.copy2.copyTo(this.instance);
				matchFunction.match(nextSpillVal, this.instance, collector);
				
				// -------- 4) cross the iterator of the spilling side with the first block --------
				while (blockIt.hasNext()) {
					PactRecord nextBlockRec = blockIt.next();
					
					// get instances of key and block value
					nextSpillVal = repeatableIter.repeatLast();
					matchFunction.match(nextSpillVal, nextBlockRec, collector);						
				}
				// reset block iterator
				blockIt.reset();
			}
			
			// if everything from the block-side fit into a single block, we are done.
			// note that in this special case, we did not create a spilling iterator at all
			if (!spillingRequired) {
				return;
			}
			
			// here we are, because we have more blocks on the block side
			// loop as long as there are blocks from the blocked input
			while (blockIt.nextBlock())
			{
				// rewind the spilling iterator
				spillIt.reset();
				
				// ------------- 5) cross the head of the spilling side with the next block ------------
				while (blockIt.hasNext()) {
					this.copy1.copyTo(this.instance);
					final PactRecord nextBlockVal = blockIt.next();
					matchFunction.match(this.instance, nextBlockVal, collector);
				}
				blockIt.reset();
				
				// -------- 6) cross the spilling iterator with the next block. ------------------
				while (spillIt.hasNext())
				{
					// get value from resettable iterator
					PactRecord nextSpillVal = spillIt.next();
					// cross value with block values
					while (blockIt.hasNext()) {
						// get instances of key and block value							
						final PactRecord nextBlockVal = blockIt.next();
						matchFunction.match(nextSpillVal, nextBlockVal, collector);	
						// get new instance of resettable value
						if (blockIt.hasNext())
							nextSpillVal = spillIt.repeatLast();
					}
					
					// reset block iterator
					blockIt.reset();
				}
				// reset v1 iterator
				spillIt.reset();
			}
		}
		finally {
			if (spillIt != null) {
				spillIt.close();
			}
		}
	}
}
