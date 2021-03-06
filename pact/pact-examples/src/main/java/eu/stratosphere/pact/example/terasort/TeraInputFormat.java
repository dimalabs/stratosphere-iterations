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

package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * This class is responsible for converting a line from the input file to a key-value pair. Lines which do not match the
 * expected length of a key-value pair are skipped.
 * 
 * @author warneke
 */
public final class TeraInputFormat extends DelimitedInputFormat {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean readRecord(PactRecord target, byte[] record, int numBytes) {

		if (numBytes != (TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE)) {
			return false;
		}

		final TeraKey key = new TeraKey(record);
		final TeraValue value = new TeraValue(record);
		
		target.setField(0, key);
		target.setField(1, value);

		return true;
	}

}
