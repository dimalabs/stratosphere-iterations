/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.plugable;

import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;


/**
 *
 *
 * @author Stephan Ewen
 */
public interface TypeAccessorsV2<T>
{
	public T createInstance();
	
	public T createCopy(T from);
	
	public void copyTo(T from, T to);
	
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 * 
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	public int getLength();
	
	// --------------------------------------------------------------------------------------------

	public long serialize(T record, DataOutputViewV2 target) throws IOException;
	
	public void deserialize(T target, DataInputViewV2 source) throws IOException;
	
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	
	public int hash(T object);
	
	public void setReferenceForEquality(T toCompare);
	
	public boolean equalToReference(T candidate);
	
	public int compare(T first, T second, Comparator<Key> comparator);
	
	public int compare(DataInputViewV2 source1, DataInputViewV2 source2) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	
	public boolean supportsNormalizedKey();
	
	public int getNormalizeKeyLen();
	
	public boolean isNormalizedKeyPrefixOnly(int keyBytes);
	
	public void putNormalizedKey(T record, byte[] target, int offset, int numBytes);
	
	// --------------------------------------------------------------------------------------------
	
	public TypeAccessorsV2<T> duplicate();
}
