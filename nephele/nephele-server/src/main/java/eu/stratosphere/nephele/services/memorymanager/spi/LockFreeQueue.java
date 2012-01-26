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

package eu.stratosphere.nephele.services.memorymanager.spi;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Stephan Ewen
 */
public class LockFreeQueue<E>
{
	private final AtomicInteger head = new AtomicInteger(0);
	
	private final AtomicInteger putHead = new AtomicInteger(0);
	
	private final AtomicInteger tail = new AtomicInteger(0);
	
	private final AtomicInteger putTail = new AtomicInteger(0);
	
	private volatile E[] elements;
	
	private final int lenMask;
	
	
	
	public LockFreeQueue(int capacity)
	{
		if (capacity < 1)
			throw new IllegalArgumentException("Capacity must be at least 1");
		if ((capacity & (capacity - 1)) != 0)
			throw new IllegalArgumentException("Capacity is not a power of 2!");
		
		this.lenMask = capacity - 1;
		
		@SuppressWarnings("unchecked")
		final E[] elems = (E[]) new Object[capacity];
		this.elements = elems;
	}
	
	
	
	public void add(E element)
	{
		int putPos = -1;
		int nextPos = -1;
		
		do {
			
			// get the put position and increment it by one to indicate the next put position
			putPos = this.putTail.get();
			nextPos = (putPos + 1) & this.lenMask;
			
			if (putPos == this.putHead.get())
				throw new IllegalArgumentException("Queue is full!");	
		} while (!this.putTail.compareAndSet(putPos, nextPos));
		
		// set the variable. we do a self write of the array in order to get a volatile write
		this.elements[putPos] = element;
		E[] elems = this.elements;
		this.elements = elems;
		
		while (!this.tail.compareAndSet(putPos, nextPos));
	}
	
	public E poll()
	{
		int readPos = -1;
		int nextPos = -1;
		
		do {
			// get the read position and check it against the tail
			readPos = this.head.get();
			nextPos = (readPos + 1) & this.lenMask;
			
			if (readPos == this.tail.get())
				return null;
			
			// make sure nobody interfered or start over
		} while (!this.head.compareAndSet(readPos, nextPos)); 
		
		// get the element
		final E element = this.elements[readPos];

		// set the variable to null. we do a self write of the array in order to get a volatile write
		E[] elems = this.elements;
		elems[readPos] = null;
		this.elements = elems;
		
		while (!this.putHead.compareAndSet(readPos, nextPos));
		
		return element;
	}
}
