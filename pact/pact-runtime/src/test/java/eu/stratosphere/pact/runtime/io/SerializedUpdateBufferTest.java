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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;


/**
 * @author Stephan Ewen 
 */
public class SerializedUpdateBufferTest
{
	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_SHORT_LENGTH = 114;
	
	private static final int VALUE_LONG_LENGTH = 112 * 1024;

	private static final int MEMORY_SEGMENT_SIZE = 64 * 1024;
	
	private static final int NUM_MEMORY_SEGMENTS = 32;
	
	private static final int MEMORY_SIZE = NUM_MEMORY_SEGMENTS * MEMORY_SEGMENT_SIZE;
	
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	// --------------------------------------------------------------------------------------------

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();
	}

	@After
	public void afterTest() {
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testWriteReadInMemory() throws Exception
	{
		final int NUM_PAIRS = 10000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		writeAndReadRecords(buffer, generator, NUM_PAIRS, false);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadInMemoryMultipleTimes() throws Exception
	{
		final int NUM_PAIRS = 8000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final TestData.Generator generator2 = new TestData.Generator(SEED+1, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		// write and read the first set of values
		writeAndReadRecords(buffer, generator, NUM_PAIRS, false);
		
		// write a second time
		writeAndReadRecords(buffer, generator2, NUM_PAIRS - 1, false);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadInMemoryMultipleTimesWithEOF() throws Exception
	{
		final int NUM_PAIRS = 8000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final TestData.Generator generator2 = new TestData.Generator(SEED+1, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		// write and read the first set of values
		writeAndReadRecords(buffer, generator, NUM_PAIRS, true);
		
		// write a second time
		writeAndReadRecords(buffer, generator2, NUM_PAIRS, true);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadWithSpilling() throws Exception
	{
		final int NUM_PAIRS = 80000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		writeAndReadRecords(buffer, generator, NUM_PAIRS, false);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadSpillingMultipleTimes() throws Exception
	{
		final int NUM_PAIRS = 80000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final TestData.Generator generator2 = new TestData.Generator(SEED+1, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		// write and read the first set of values
		writeAndReadRecords(buffer, generator, NUM_PAIRS, false);
		
		// write a second time
		writeAndReadRecords(buffer, generator2, NUM_PAIRS - 1, false);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadSpillingMultipleTimesWithEOF() throws Exception
	{
		final int NUM_PAIRS = 80000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final TestData.Generator generator2 = new TestData.Generator(SEED+1, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		// write and read the first set of values
		writeAndReadRecords(buffer, generator, NUM_PAIRS, true);
		
		// write a second time
		writeAndReadRecords(buffer, generator2, NUM_PAIRS, true);
		
		this.memoryManager.release(buffer.close());
	}
	
	@Test
	public void testWriteReadLongRecords() throws Exception
	{
		final int NUM_PAIRS = 3000;
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final TestData.Generator generator2 = new TestData.Generator(SEED+1, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memory, MEMORY_SEGMENT_SIZE, this.ioManager);
		
		// write and read the first set of values
		writeAndReadRecords(buffer, generator, NUM_PAIRS, false);
		
		// write a second time
		writeAndReadRecords(buffer, generator2, NUM_PAIRS - 1, false);
		
		this.memoryManager.release(buffer.close());
	}
	
	private final void writeAndReadRecords(SerializedUpdateBuffer buffer, TestData.Generator generator, int num, boolean checkEndWithEOF)
	throws IOException
	{
		// write a number of pairs
		final PactRecord rec = new PactRecord();
		for (int i = 0; i < num; i++) {
			generator.next(rec);
			rec.write(buffer);
		}
		
		// create the reader input view
		DataInputViewV2 inView = buffer.switchBuffers();
		generator.reset();
		
		// read and re-generate all records and compare them
		final PactRecord readRec = new PactRecord();
		
		if (checkEndWithEOF) {
			int count = 0;
			try {
				while (true) {
					generator.next(rec);
					readRec.read(inView);
					count++;
					
					Key k1 = rec.getField(0, Key.class);
					Value v1 = rec.getField(1, Value.class);
					
					Key k2 = readRec.getField(0, Key.class);
					Value v2 = readRec.getField(1, Value.class);
					
					Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
				}
			} catch (EOFException eofex) {
				// expected
			}
			Assert.assertEquals("Read and retrieved number of values differ.", num, count);
		}
		else {
			for (int i = 0; i < num; i++) {
				generator.next(rec);
				readRec.read(inView);
				
				Key k1 = rec.getField(0, Key.class);
				Value v1 = rec.getField(1, Value.class);
				
				Key k2 = readRec.getField(0, Key.class);
				Value v2 = readRec.getField(1, Value.class);
				
				Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
			}
		}
	}
	
//	@Test
//	public void testWriteAndReadLongRecords() throws Exception
//	{
//		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
//		final Channel.ID channel = this.ioManager.createChannel();
//		
//		// create the writer output view
//		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel);
//		final ChannelWriterOutputViewV2 outView = new ChannelWriterOutputViewV2(writer, memory, MEMORY_SEGMENT_SIZE);
//		
//		// write a number of pairs
//		final PactRecord rec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_LONG; i++) {
//			generator.next(rec);
//			rec.write(outView);
//		}
//		this.memoryManager.release(outView.close());
//		
//		// create the reader input view
//		memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channel);
//		final ChannelReaderInputViewV2 inView = new ChannelReaderInputViewV2(reader, memory, outView.getBlockCount(), true);
//		generator.reset();
//		
//		// read and re-generate all records and compare them
//		final PactRecord readRec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_LONG; i++) {
//			generator.next(rec);
//			readRec.read(inView);
//			final Key k1 = rec.getField(0, Key.class);
//			final Value v1 = rec.getField(1, Value.class);
//			final Key k2 = readRec.getField(0, Key.class);
//			final Value v2 = readRec.getField(1, Value.class);
//			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
//		}
//		
//		this.memoryManager.release(inView.close());
//		reader.deleteChannel();
//	}
//	
//	@Test
//	public void testReadTooMany() throws Exception
//	{
//		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
//		final Channel.ID channel = this.ioManager.createChannel();
//		
//		// create the writer output view
//		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel);
//		final ChannelWriterOutputViewV2 outView = new ChannelWriterOutputViewV2(writer, memory, MEMORY_SEGMENT_SIZE);
//		
//		// write a number of pairs
//		final PactRecord rec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
//			generator.next(rec);
//			rec.write(outView);
//		}
//		this.memoryManager.release(outView.close());
//		
//		// create the reader input view
//		memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channel);
//		final ChannelReaderInputViewV2 inView = new ChannelReaderInputViewV2(reader, memory, outView.getBlockCount(), true);
//		generator.reset();
//		
//		// read and re-generate all records and compare them
//		try {
//			final PactRecord readRec = new PactRecord();
//			for (int i = 0; i < NUM_PAIRS_SHORT + 1; i++) {
//				generator.next(rec);
//				readRec.read(inView);
//				final Key k1 = rec.getField(0, Key.class);
//				final Value v1 = rec.getField(1, Value.class);
//				final Key k2 = readRec.getField(0, Key.class);
//				final Value v2 = readRec.getField(1, Value.class);
//				Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
//			}
//			Assert.fail("Expected an EOFException which did not occur.");
//		}
//		catch (EOFException eofex) {
//			// expected
//		}
//		catch (Throwable t) {
//			// unexpected
//			Assert.fail("Unexpected Exception: " + t.getMessage());
//		}
//		
//		this.memoryManager.release(inView.close());
//		reader.deleteChannel();
//	}
//	
//	@Test
//	public void testReadWithoutKnownBlockCount() throws Exception
//	{
//		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
//		final Channel.ID channel = this.ioManager.createChannel();
//		
//		// create the writer output view
//		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel);
//		final ChannelWriterOutputViewV2 outView = new ChannelWriterOutputViewV2(writer, memory, MEMORY_SEGMENT_SIZE);
//		
//		// write a number of pairs
//		final PactRecord rec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
//			generator.next(rec);
//			rec.write(outView);
//		}
//		this.memoryManager.release(outView.close());
//		
//		// create the reader input view
//		memory = this.memoryManager.allocateStrict(this.parentTask, NUM_MEMORY_SEGMENTS, MEMORY_SEGMENT_SIZE);
//		final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channel);
//		final ChannelReaderInputViewV2 inView = new ChannelReaderInputViewV2(reader, memory, true);
//		generator.reset();
//		
//		// read and re-generate all records and cmpare them
//		final PactRecord readRec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
//			generator.next(rec);
//			readRec.read(inView);
//			
//			Key k1 = rec.getField(0, Key.class);
//			Value v1 = rec.getField(1, Value.class);
//			
//			Key k2 = readRec.getField(0, Key.class);
//			Value v2 = readRec.getField(1, Value.class);
//			
//			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
//		}
//		
//		this.memoryManager.release(inView.close());
//		reader.deleteChannel();
//	}
//	
//	@Test
//	public void testWriteReadOneBufferOnly() throws Exception
//	{
//		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
//		final Channel.ID channel = this.ioManager.createChannel();
//		
//		// create the writer output view
//		List<MemorySegment> memory = this.memoryManager.allocateStrict(this.parentTask, 1, MEMORY_SEGMENT_SIZE);
//		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel);
//		final ChannelWriterOutputViewV2 outView = new ChannelWriterOutputViewV2(writer, memory, MEMORY_SEGMENT_SIZE);
//		
//		// write a number of pairs
//		final PactRecord rec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
//			generator.next(rec);
//			rec.write(outView);
//		}
//		this.memoryManager.release(outView.close());
//		
//		// create the reader input view
//		memory = this.memoryManager.allocateStrict(this.parentTask, 1, MEMORY_SEGMENT_SIZE);
//		final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channel);
//		final ChannelReaderInputViewV2 inView = new ChannelReaderInputViewV2(reader, memory, outView.getBlockCount(), true);
//		generator.reset();
//		
//		// read and re-generate all records and compare them
//		final PactRecord readRec = new PactRecord();
//		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
//			generator.next(rec);
//			readRec.read(inView);
//			
//			Key k1 = rec.getField(0, Key.class);
//			Value v1 = rec.getField(1, Value.class);
//			
//			Key k2 = readRec.getField(0, Key.class);
//			Value v2 = readRec.getField(1, Value.class);
//			
//			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
//		}
//		
//		this.memoryManager.release(inView.close());
//		reader.deleteChannel();
//	}
}