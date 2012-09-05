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

package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Parses a decimal text field into a PactInteger.
 * Only characters '1' to '0' and '-' are allowed.
 * The parser does not check for the maximum value.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class DecimalTextIntParser  implements FieldParser<PactInteger>
{
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration config) { }
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#parseField(byte[], int, int, char, eu.stratosphere.pact.common.type.Value)
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, PactInteger field)
	{
		int val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
		}
		
		for (int i = startPos; i < limit; i++) {
			if (bytes[i] == delim) {
				field.setValue(val*(neg ? -1 : 1));
				return i+1;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
		}
		field.setValue(val*(neg ? -1 : 1));
		return limit;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#getValue()
	 */
	@Override
	public PactInteger getValue() {
		return new PactInteger();
	}
}
