package eu.stratosphere.pact.iterative.nephele.util.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.sort.PartialOrderPriorityQueue;


public class RawMergeIterator implements MutableObjectIterator<Value>
{
	private final PartialOrderPriorityQueue<HeadStream> heap;

	
	public RawMergeIterator(List<MutableObjectIterator<Value>> iterators, 
			TypeAccessorsV2<Value> accessor)
	throws IOException
	{
		this.heap = new PartialOrderPriorityQueue<HeadStream>(new HeadStreamComparator(comparators), iterators.size());
		
		for (MutableObjectIterator<Value> iterator : iterators) {
			heap.add(new HeadStream(iterator, accessor));
		}
	}

	@Override
	public boolean next(Value target) throws IOException
	{
		if (this.heap.size() > 0) {
			// get the smallest element
			HeadStream top = heap.peek();
			top.getHead().copyTo(target);
			
			// read an element
			if (!top.nextHead()) {
				heap.poll();
			}
			heap.adjustTop();
			return true;
		}
		else {
			return false;
		}
	}

	// ============================================================================================
	//                      Internal Classes that wrap the sorted input streams
	// ============================================================================================
	
	private static final class HeadStream
	{
		private final MutableObjectIterator<PactRecord> iterator;

		private final Key[] keyHolders;
		
		private final int[] keyPositions;
		
		private final PactRecord head = new PactRecord();

		public HeadStream(MutableObjectIterator<PactRecord> iterator, int[] keyPositions, Class<? extends Key>[] keyClasses)
		throws IOException
		{
			this.iterator = iterator;
			this.keyPositions = keyPositions;
		
			// instantiate the array that caches the key objects
			this.keyHolders = new Key[keyClasses.length];
			for (int i = 0; i < keyClasses.length; i++) {
				if (keyClasses[i] == null) {
					throw new NullPointerException("Key type " + i + " is null.");
				}
				this.keyHolders[i] = InstantiationUtil.instantiate(keyClasses[i], Key.class);
			}
			
			if (!nextHead())
				throw new IllegalStateException();
		}

		public PactRecord getHead() {
			return this.head;
		}

		public boolean nextHead() throws IOException {
			if (iterator.next(this.head)) {
				this.head.getFieldsInto(this.keyPositions, this.keyHolders);
				return true;
			}
			else {
				return false;
			}
		}
	}
}