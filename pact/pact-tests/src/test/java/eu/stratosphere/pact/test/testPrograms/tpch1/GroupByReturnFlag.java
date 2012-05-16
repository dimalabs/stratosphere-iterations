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

package eu.stratosphere.pact.test.testPrograms.tpch1;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

/**
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 * @author Moritz Kaufmann <moritz.kaufmann@campus.tu-berlin.de>
 */
public class GroupByReturnFlag extends ReduceStub {

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
		PactRecord outRecord = new PactRecord();
		Tuple returnTuple = new Tuple();
		
		long quantity = 0;
		double extendedPriceSum = 0.0; 
		
		boolean first = true;
		while(records.hasNext()) {
			PactRecord rec = records.next();
			Tuple t = rec.getField(1, Tuple.class);
			
			if(first) {
				first = false;
				rec.copyTo(outRecord);
				returnTuple.addAttribute(rec.getField(0, PactString.class).toString());
			}
			
			long tupleQuantity = Long.parseLong(t.getStringValueAt(4));
			quantity += tupleQuantity;
			
			double extendedPricePerTuple = Double.parseDouble(t.getStringValueAt(5));
			extendedPriceSum += extendedPricePerTuple;
		}
		
		PactLong pactQuantity = new PactLong(quantity);
		returnTuple.addAttribute("" + pactQuantity);
		returnTuple.addAttribute("" + extendedPriceSum);
		
		outRecord.setField(1, returnTuple);
		out.collect(outRecord);
	}

}
