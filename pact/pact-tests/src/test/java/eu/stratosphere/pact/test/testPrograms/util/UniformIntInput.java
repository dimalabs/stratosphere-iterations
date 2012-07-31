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

package eu.stratosphere.pact.test.testPrograms.util;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.GenericInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * 
 */
public class UniformIntInput extends GenericInputFormat
{
	public static final String NUM_KEYS_KEY = "testfomat.numkeys";
	public static final String NUM_VALUES_KEY = "testfomat.numvalues";
	
	private static final int DEFAULT_NUM_KEYS = 1000;
	private static final int DEFAULT_NUM_VALUES = 1000;
	
	private final PactInteger key = new PactInteger();
	private final PactInteger value = new PactInteger();
	
	private int numKeys;
	private int numValues;
	
	private int keyInt;
	private int valueInt;

	public UniformIntInput() {
		this(DEFAULT_NUM_KEYS, DEFAULT_NUM_VALUES);
	}
	
	public UniformIntInput(final int numKeys, final int numValues) {
		this.numKeys = numKeys;
		this.numValues = numValues;
	}
	
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.GenericInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		this.numKeys = parameters.getInteger(NUM_KEYS_KEY, this.numKeys);
		this.numValues = parameters.getInteger(NUM_VALUES_KEY, this.numValues);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#reachedEnd()
	 */
	@Override
	public boolean reachedEnd() {
		return this.valueInt >= this.numValues;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException
	{
		if (this.keyInt == this.numKeys) {
			this.keyInt = 0;
			this.valueInt++;
		}

		this.key.setValue(this.keyInt);
		this.value.setValue(this.valueInt);
		
		record.setField(0, this.key);
		record.setField(1, this.value);
		record.updateBinaryRepresenation();
		
		this.keyInt++;
		
		return true;
	}
}
