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

package eu.stratosphere.pact.test.testPrograms.tpch9;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

public class PartJoin extends MatchStub {
	
	private final Tuple partSuppValue = new Tuple();
	private final PactInteger partKey = new PactInteger();
	
	/**
	 * Join "part" and "partsupp" by "partkey".
	 * 
	 * Output Schema:
	 *  Key: (partkey, suppkey)
	 *  Value: supplycost
	 *
	 */
	@Override
	public void match(PactRecord value1, PactRecord value2, Collector out)
			throws Exception {

		PactInteger partKey = value1.getField(0, this.partKey);
		Tuple partSuppValue = value2.getField(1, this.partSuppValue);
		
		IntPair newKey = new IntPair(partKey, new PactInteger(Integer.parseInt(partSuppValue.getStringValueAt(0))));
		String supplyCost = partSuppValue.getStringValueAt(1);
		
		value1.setField(0, newKey);
		value1.setField(1, new PactString(supplyCost));
		out.collect(value1);
		
	}

}
