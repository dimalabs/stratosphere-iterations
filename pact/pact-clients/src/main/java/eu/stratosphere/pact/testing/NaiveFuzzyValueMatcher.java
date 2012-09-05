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

package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <PactRecord>
 */
public class NaiveFuzzyValueMatcher extends AbstractValueMatcher {
	@Override
	public void removeMatchingValues(Int2ObjectMap<List<ValueSimilarity<?>>> similarities,
			Class<? extends Value>[] schema, Collection<PactRecord> expectedValues, Collection<PactRecord> actualValues) {
		Iterator<PactRecord> expectedIterator = expectedValues.iterator();
		List<PactRecord> matchedActualValues = new ArrayList<PactRecord>();
		while (expectedIterator.hasNext()) {
			PactRecord expected = expectedIterator.next();
			boolean matched = false;
			for (PactRecord actual : actualValues)
				if (this.getDistance(schema, similarities, expected, actual) >= 0) {
					matched = true;
					matchedActualValues.add(actual);
				}
			if (matched)
				expectedIterator.remove();
		}

		actualValues.removeAll(matchedActualValues);
	}

}
