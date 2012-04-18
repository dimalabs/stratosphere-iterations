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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.sort.DeserializerComparator;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.util.KeyComparator;

/**
 * @author Erik Nijkamp
 */
public class DeserializerComparatorTest {

	@Test
	public void testCompareSingleKeyAt0() throws Exception
	{
		// initialize comparator
		Comparator<Key> keyComparator = new KeyComparator();
		
		@SuppressWarnings("unchecked")
		RawComparator rawComparator = new DeserializerComparator(new int[]{0}, 
			new Class[]{TestData.Key.class}, new Comparator[]{keyComparator});

		// sample data
		TestData.Key key1 = new TestData.Key(10);
		TestData.Value val1 = new TestData.Value("Some-test-value-here.");
		PactRecord rec1 = new PactRecord(key1, val1);
		
		TestData.Key key2 = new TestData.Key(20);
		TestData.Value val2 = new TestData.Value("Another-magic-test-value.");
		PactRecord rec2 = new PactRecord(key2, val2);
		
		// serialize
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream dataOutput1 = new DataOutputStream(buffer1);
		rec1.write(dataOutput1);
		byte[] key1bytes = buffer1.toByteArray();

		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream dataOutput2 = new DataOutputStream(buffer2);
		rec2.write(dataOutput2);
		byte[] key2bytes = buffer2.toByteArray();

		// assertion
		Assert.assertTrue(rawComparator.compare(key1bytes, key2bytes, 0, 0) < 0);
		Assert.assertTrue(rawComparator.compare(key2bytes, key1bytes, 0, 0) > 0);
		Assert.assertTrue(rawComparator.compare(key1bytes, key1bytes, 0, 0) == 0);
	}
	
	@Test
	public void testCompareSingleKeyAt1() throws Exception
	{
		// initialize comparator
		Comparator<Key> keyComparator = new KeyComparator();
		
		@SuppressWarnings("unchecked")
		RawComparator rawComparator = new DeserializerComparator(new int[]{1}, 
			new Class[]{TestData.Key.class}, new Comparator[]{keyComparator});

		// sample data
		TestData.Key key1 = new TestData.Key(10);
		TestData.Value val1 = new TestData.Value("Some-test-value-here.");
		PactRecord rec1 = new PactRecord(val1, key1);
		
		TestData.Key key2 = new TestData.Key(20);
		TestData.Value val2 = new TestData.Value("Another-magic-test-value.");
		PactRecord rec2 = new PactRecord(val2, key2);
		
		// serialize
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream dataOutput1 = new DataOutputStream(buffer1);
		rec1.write(dataOutput1);
		byte[] key1bytes = buffer1.toByteArray();

		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream dataOutput2 = new DataOutputStream(buffer2);
		rec2.write(dataOutput2);
		byte[] key2bytes = buffer2.toByteArray();

		// assertion
		Assert.assertTrue(rawComparator.compare(key1bytes, key2bytes, 0, 0) < 0);
		Assert.assertTrue(rawComparator.compare(key2bytes, key1bytes, 0, 0) > 0);
		Assert.assertTrue(rawComparator.compare(key1bytes, key1bytes, 0, 0) == 0);
	}
	
	@Test
	public void testCompareCompositeKey() throws Exception
	{
		// initialize comparator
		Comparator<Key> keyComparator = new KeyComparator();
		
		@SuppressWarnings("unchecked")
		RawComparator rawComparator = new DeserializerComparator(new int[]{2, 0}, 
			new Class[]{TestData.Key.class, PactInteger.class}, new Comparator[]{keyComparator, keyComparator});

		// sample data
		PactInteger int1 = new PactInteger(18);
		TestData.Key key1 = new TestData.Key(10);
		TestData.Value val1 = new TestData.Value("Some-test-value-here.");
		PactRecord rec1 = new PactRecord(int1, val1);
		rec1.setField(2, key1);
		
		PactInteger int2 = new PactInteger(20);
		TestData.Key key2 = new TestData.Key(11);
		TestData.Value val2 = new TestData.Value("Another-magic-test-value.");
		PactRecord rec2 = new PactRecord(int2, val2);
		rec2.setField(2, key2);
		
		// serialize
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream dataOutput1 = new DataOutputStream(buffer1);
		rec1.write(dataOutput1);
		byte[] key1bytes = buffer1.toByteArray();

		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream dataOutput2 = new DataOutputStream(buffer2);
		rec2.write(dataOutput2);
		byte[] key2bytes = buffer2.toByteArray();

		// assertion
		Assert.assertTrue(rawComparator.compare(key1bytes, key2bytes, 0, 0) < 0);
		Assert.assertTrue(rawComparator.compare(key2bytes, key1bytes, 0, 0) > 0);
		Assert.assertTrue(rawComparator.compare(key1bytes, key1bytes, 0, 0) == 0);
	}
	
	@Test
	public void testCompareCompositeKey2() throws Exception
	{
		// initialize comparator
		Comparator<Key> keyComparator = new KeyComparator();
		
		@SuppressWarnings("unchecked")
		RawComparator rawComparator = new DeserializerComparator(new int[]{2, 0}, 
			new Class[]{TestData.Key.class, PactInteger.class}, new Comparator[]{keyComparator, keyComparator});

		// sample data
		PactInteger int1 = new PactInteger(18);
		TestData.Key key1 = new TestData.Key(11);
		TestData.Value val1 = new TestData.Value("Some-test-value-here.");
		PactRecord rec1 = new PactRecord(int1, val1);
		rec1.setField(2, key1);
		
		PactInteger int2 = new PactInteger(20);
		TestData.Key key2 = new TestData.Key(11);
		TestData.Value val2 = new TestData.Value("Another-magic-test-value.");
		PactRecord rec2 = new PactRecord(int2, val2);
		rec2.setField(2, key2);
		
		// serialize
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream dataOutput1 = new DataOutputStream(buffer1);
		rec1.write(dataOutput1);
		byte[] key1bytes = buffer1.toByteArray();

		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream dataOutput2 = new DataOutputStream(buffer2);
		rec2.write(dataOutput2);
		byte[] key2bytes = buffer2.toByteArray();

		// assertion
		Assert.assertTrue(rawComparator.compare(key1bytes, key2bytes, 0, 0) < 0);
		Assert.assertTrue(rawComparator.compare(key2bytes, key1bytes, 0, 0) > 0);
		Assert.assertTrue(rawComparator.compare(key1bytes, key1bytes, 0, 0) == 0);
	}
}
