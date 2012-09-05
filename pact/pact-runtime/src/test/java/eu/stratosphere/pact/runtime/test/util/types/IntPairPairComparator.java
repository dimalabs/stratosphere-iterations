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

package eu.stratosphere.pact.runtime.test.util.types;

import eu.stratosphere.pact.common.generic.types.TypePairComparator;


/**
 * @author Stephan Ewen
 */
public class IntPairPairComparator implements TypePairComparator<IntPair, IntPair>
{
	private int key;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
	 */
	@Override
	public void setReference(IntPair reference) {
		this.key = reference.getKey();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalToReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
	 */
	@Override
	public boolean equalToReference(IntPair candidate) {
		return this.key == candidate.getKey();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypePairComparator#compareToReference(java.lang.Object)
	 */
	@Override
	public int compareToReference(IntPair candidate) {
		return candidate.getKey() - this.key;
	}
	
}