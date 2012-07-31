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

package eu.stratosphere.pact.example.graph.io;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;


/**
 * @author Stephan Ewen
 */
public final class EdgeInputFormat extends DelimitedInputFormat
{
	public static final String ID_DELIMITER_CHAR = "edgeinput.delimiter";
	
	private final PactInteger i1 = new PactInteger();
	private final PactInteger i2 = new PactInteger();
	
	private char delimiter;
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
	 */
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
	{
		final int limit = offset + numBytes;
		int first = 0, second = 0;
		final char delimiter = this.delimiter;
		
		int pos = offset;
		while (pos < limit && bytes[pos] != delimiter) {
			first = first * 10 + (bytes[pos++] - '0');
		}
		pos += 1;// skip the delimiter
		while (pos < limit) {
			second = second * 10 + (bytes[pos++] - '0');
		}
		
		if (first <= 0 || second <= 0 || first == second)
			return false;
		
		this.i1.setValue(first);
		this.i2.setValue(second);
		target.setField(0, this.i1);
		target.setField(1, this.i2);
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		this.delimiter = (char) parameters.getInteger(ID_DELIMITER_CHAR, ',');
	}
}