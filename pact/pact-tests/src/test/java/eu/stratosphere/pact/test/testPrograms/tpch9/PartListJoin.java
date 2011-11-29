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

public class PartListJoin extends MatchStub{

	private final StringIntPair amountYearPair = new StringIntPair();
	private final PactString nationName = new PactString();
	
	/**
	 * Join "filteredParts" and "suppliers" by "suppkey".
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	@Override
	public void match(PactRecord value1, PactRecord value2, Collector out) throws Exception
	{
		StringIntPair amountYearPair = value1.getField(1, this.amountYearPair);
		PactString nationName = value2.getField(1, this.nationName);
		
		PactInteger year = amountYearPair.getSecond();
		PactString amount = amountYearPair.getFirst();
		StringIntPair key = new StringIntPair(nationName, year);
		value1.setField(0, key);
		value1.setField(1, amount);
		out.collect(value1);
	}

}
