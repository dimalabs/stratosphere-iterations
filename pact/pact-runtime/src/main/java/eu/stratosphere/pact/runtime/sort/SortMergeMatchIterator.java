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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that realizes the
 * matching through a sort-merge join strategy.
 *
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class SortMergeMatchIterator<T1, T2, O> implements MatchTaskIterator<T1, T2, O>
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
	
	private TypePairComparator<T1, T2> comp;
	
	private KeyGroupedIterator<T1> iterator1;

	private KeyGroupedIterator<T2> iterator2;
	
	private final TypeSerializer<T1> serializer1;
	
	private final TypeSerializer<T2> serializer2;
	
	private final T1 copy1;
	
	private final T1 spillHeadCopy;
	
	private final T2 copy2;
	
	private final T2 blockHeadCopy;
	
	private final BlockResettableIterator<T2> blockIt;				// for N:M cross products with same key
	
	private final List<MemorySegment> memoryForSpillingIterator;
	
	private final MutableObjectIterator<T1> reader1;

	private final MutableObjectIterator<T2> reader2;
	
	private final TypeComparator<T1> comparator1;
	
	private final TypeComparator<T2> comparator2;
	
	private Sorter<T1> sortMerger1;

	private Sorter<T2> sortMerger2;
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;
	
	private final LocalStrategy localStrategy;
	
	private final AbstractInvokable parentTask;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;
	

	
	public SortMergeMatchIterator(MutableObjectIterator<T1> reader1, MutableObjectIterator<T2> reader2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2, TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager, IOManager ioManager,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this(reader1, reader2, serializer1, comparator1, serializer2, comparator2, pairComparator,
			memoryManager, ioManager, 
			memory, maxNumFileHandles, spillingThreshold, DEFAULT_MEMORY_SHARE_RATIO, 
			localStrategy, parentTask);
	}
	
	public SortMergeMatchIterator(MutableObjectIterator<T1> reader1, MutableObjectIterator<T2> reader2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2, TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager, IOManager ioManager,
			long memory, int maxNumFileHandles, float spillingThreshold, float memPercentageForBlockNL,
			LocalStrategy localStrategy, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this.comp = pairComparator;
		this.serializer1 = serializer1;
		this.serializer2 = serializer2;
		this.comparator1 = comparator1;
		this.comparator2 = comparator2;
		
		this.copy1 = serializer1.createInstance();
		this.spillHeadCopy = serializer1.createInstance();
		this.copy2 = serializer2.createInstance();
		this.blockHeadCopy = serializer2.createInstance();
		
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.reader1 = reader1;
		this.reader2 = reader2;
		
		final int pageSize = memoryManager.getPageSize();
		long memoryForBlockNestedLoops = Math.max((long) (memory * memPercentageForBlockNL), 2 * pageSize);
		long  pagesForBlockNL = memoryForBlockNestedLoops / pageSize;
		int numPagesForSpiller = pagesForBlockNL > 20 ? 2 : 1;
		
		this.memoryPerChannel = (memory - memoryForBlockNestedLoops) / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
		
		this.blockIt = new BlockResettableIterator<T2>(this.memoryManager, this.serializer2, 
			memoryForBlockNestedLoops - (numPagesForSpiller * pageSize), parentTask);
		this.memoryForSpillingIterator = memoryManager.allocatePages(parentTask, numPagesForSpiller);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{	
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
			this.sortMerger1 = new UnilateralSortMerger<T1>(this.memoryManager, this.ioManager,
					this.reader1, this.parentTask, this.serializer1, this.comparator1, 
					this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}

		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			this.sortMerger2 = new UnilateralSortMerger<T2>(this.memoryManager, this.ioManager,
					this.reader2, this.parentTask, this.serializer2, this.comparator2, 
					this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}
			
		// =============== These calls freeze until the data is actually available ============ 
		
		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.comparator2.duplicate());
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.comparator2.duplicate());
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.comparator2.duplicate());
				break;
			case MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.comparator2.duplicate());
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
		
		this.memoryManager.release(this.memoryForSpillingIterator);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#abort()
	 */
	@Override
	public void abort()
	{
		close();
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
	public boolean callWithNextKey(final GenericMatcher<T1, T2, O> matchFunction, final Collector<O> collector)
	throws Exception
	{
		if (!this.iterator1.nextKey() || !this.iterator2.nextKey()) {
			return false;
		}

		final TypePairComparator<T1, T2> comparator = this.comp;
		comparator.setReference(this.iterator1.getCurrent());
		T2 current2 = this.iterator2.getCurrent();
				
		// zig zag
		while (true) {
			// determine the relation between the (possibly composite) keys
			final int comp = comparator.compareToReference(current2);
			
			if (comp == 0)
				break;
			
			if (comp < 0) {
				if (!this.iterator2.nextKey()) {
					return false;
				}
				current2 = this.iterator2.getCurrent();
			}
			else {
				if (!this.iterator1.nextKey()) {
					return false;
				}
				comparator.setReference(this.iterator1.getCurrent());
			}
		}
		
		// here, we have a common key! call the match function with the cross product of the
		// values
		final KeyGroupedIterator<T1>.ValuesIterator values1 = this.iterator1.getValues();
		final KeyGroupedIterator<T2>.ValuesIterator values2 = this.iterator2.getValues();
		
		final T1 firstV1 = values1.next();
		final T2 firstV2 = values2.next();	
			
		final boolean v1HasNext = values1.hasNext();
		final boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		
		if (v1HasNext) {
			if (v2HasNext) {
				// both sides contain more than one value
				// TODO: Decide which side to spill and which to block!
				crossMwithNValues(firstV1, values1, firstV2, values2, matchFunction, collector);
			} else {
				crossSecond1withNValues(firstV2, firstV1, values1, matchFunction, collector);
			}
		} else {
			if (v2HasNext) {
				crossFirst1withNValues(firstV1, firstV2, values2, matchFunction, collector);
			} else {
				// both sides contain only one value
				matchFunction.match(firstV1, firstV2, collector);
			}
		}
		return true;
	}

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
	private void crossFirst1withNValues(final T1 val1, final T2 firstValN,
			final Iterator<T2> valsN, final GenericMatcher<T1, T2, O> matchFunction, final Collector<O> collector)
	throws Exception
	{
		this.serializer1.copyTo(val1, this.copy1);
		matchFunction.match(this.copy1, firstValN, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			final T2 nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.serializer1.copyTo(val1, this.copy1);
				matchFunction.match(this.copy1, nRec, collector);
			} else {
				matchFunction.match(val1, nRec, collector);
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
	private void crossSecond1withNValues(T2 val1, T1 firstValN,
			Iterator<T1> valsN, GenericMatcher<T1, T2, O> matchFunction, Collector<O> collector)
	throws Exception
	{
		this.serializer2.copyTo(val1, this.copy2);
		matchFunction.match(firstValN, this.copy2, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			final T1 nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.serializer2.copyTo(val1, this.copy2);
				matchFunction.match(nRec, this.copy2, collector);
			} else {
				matchFunction.match(nRec, val1, collector);
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
	private void crossMwithNValues(final T1 firstV1, Iterator<T1> spillVals,
			final T2 firstV2, final Iterator<T2> blockVals,
			final GenericMatcher<T1, T2, O> matchFunction, final Collector<O> collector)
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
		this.serializer1.copyTo(firstV1, this.copy1);
		this.serializer2.copyTo(firstV2, this.blockHeadCopy);
		
		// --------------- 1) Cross the heads -------------------
		matchFunction.match(this.copy1, firstV2, collector);
		
		// for the remaining values, we do a block-nested-loops join
		SpillingResettableIterator<T1> spillIt = null;
		
		try {
			// create block iterator on the second input
			this.blockIt.reopen(blockVals);
			
			// ------------- 2) cross the head of the spilling side with the first block ------------------
			while (this.blockIt.hasNext()) {
				final T2 nextBlockRec = this.blockIt.next();
				this.serializer1.copyTo(firstV1, this.copy1);
				matchFunction.match(this.copy1, nextBlockRec, collector);
			}
			this.blockIt.reset();
			
			// spilling is required if the blocked input has data beyond the current block.
			// in that case, create the spilling iterator
			final Iterator<T1> leftSideIter;
			final boolean spillingRequired = this.blockIt.hasFurtherInput();
			if (spillingRequired)
			{
				// more data than would fit into one block. we need to wrap the other side in a spilling iterator
				// create spilling iterator on first input
				spillIt = new SpillingResettableIterator<T1>(spillVals, this.serializer1,
						this.memoryManager, this.ioManager, this.memoryForSpillingIterator);
				leftSideIter = spillIt;				
				spillIt.open();
				
				this.serializer1.copyTo(firstV1, this.spillHeadCopy);
			}
			else {
				leftSideIter = spillVals;
			}
			
			// cross the values in the v1 iterator against the current block
			
			while (leftSideIter.hasNext()) {
				final T1 nextSpillVal = leftSideIter.next();
				this.serializer1.copyTo(nextSpillVal, this.copy1);
				
				
				// -------- 3) cross the iterator of the spilling side with the head of the block side --------
				this.serializer2.copyTo(this.blockHeadCopy, this.copy2);
				matchFunction.match(this.copy1, this.copy2, collector);
				
				// -------- 4) cross the iterator of the spilling side with the first block --------
				while (this.blockIt.hasNext()) {
					T2 nextBlockRec = this.blockIt.next();
					
					// get instances of key and block value
					this.serializer1.copyTo(nextSpillVal, this.copy1);
					matchFunction.match(this.copy1, nextBlockRec, collector);						
				}
				// reset block iterator
				this.blockIt.reset();
			}
			
			// if everything from the block-side fit into a single block, we are done.
			// note that in this special case, we did not create a spilling iterator at all
			if (!spillingRequired) {
				return;
			}
			
			// here we are, because we have more blocks on the block side
			// loop as long as there are blocks from the blocked input
			while (this.blockIt.nextBlock())
			{
				// rewind the spilling iterator
				spillIt.reset();
				
				// ------------- 5) cross the head of the spilling side with the next block ------------
				while (this.blockIt.hasNext()) {
					this.serializer1.copyTo(this.spillHeadCopy, this.copy1);
					final T2 nextBlockVal = blockIt.next();
					matchFunction.match(this.copy1, nextBlockVal, collector);
				}
				this.blockIt.reset();
				
				// -------- 6) cross the spilling iterator with the next block. ------------------
				while (spillIt.hasNext())
				{
					// get value from resettable iterator
					final T1 nextSpillVal = spillIt.next();
					// cross value with block values
					while (this.blockIt.hasNext()) {
						// get instances of key and block value
						final T2 nextBlockVal = this.blockIt.next();
						this.serializer1.copyTo(nextSpillVal, this.copy1);
						matchFunction.match(this.copy1, nextBlockVal, collector);	
					}
					
					// reset block iterator
					this.blockIt.reset();
				}
				// reset v1 iterator
				spillIt.reset();
			}
		}
		finally {
			if (spillIt != null) {
				this.memoryForSpillingIterator.addAll(spillIt.close());
			}
		}
	}
}
