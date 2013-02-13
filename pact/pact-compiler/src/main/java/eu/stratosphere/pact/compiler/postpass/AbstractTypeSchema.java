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

package eu.stratosphere.pact.compiler.postpass;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.Value;

/**
 * Class encapsulating a schema map (int column position -> column type) and a reference counter.
 */
abstract class AbstractTypeSchema<T extends Value> implements Iterable<Integer> {
	
	private final Map<Integer, Class<? extends T>> schema;
	
	private final Map<Integer, Integer> reMapping;	
	
	private int numConnectionsThatContributed;
	
	
	public AbstractTypeSchema() {
		this.schema = new HashMap<Integer, Class<? extends T>>();
		this.reMapping = new HashMap<Integer, Integer>();
	}
	
	public void addSchema(AbstractTypeSchema<T> otherSchema) throws ConflictingFieldTypeInfoException {
		for (Map.Entry<Integer, Class<? extends T>> entry : otherSchema.schema.entrySet()) {
			Class<? extends T> previous = this.schema.put(entry.getKey(), entry.getValue());
			if (previous != null && previous != entry.getValue()) {
				throw new ConflictingFieldTypeInfoException(entry.getKey(), previous, entry.getValue());
			}
		}
		
		this.reMapping.putAll(otherSchema.reMapping);
	}
	
	public void addType(Integer key, Integer reMappedKey, Class<? extends T> type) throws ConflictingFieldTypeInfoException 
	{
		Class<? extends T> previous = this.schema.put(key, type);
		if (previous != null && previous != type) {
			throw new ConflictingFieldTypeInfoException(key, previous, type);
		}
		this.reMapping.put(key, reMappedKey);
	}
	
	public Class<? extends T> getType(Integer field) {
		return this.schema.get(field);
	}
	
	public Integer getRemappedPosition(Integer key) {
		return this.reMapping.get(key);
	}
	
	public Iterator<Integer> iterator() {
		return this.schema.keySet().iterator();
	}
	
	public int getNumConnectionsThatContributed() {
		return this.numConnectionsThatContributed;
	}
	
	public void increaseNumConnectionsThatContributed() {
		this.numConnectionsThatContributed++;
	}


	@Override
	public int hashCode() {
		return this.schema.hashCode() ^ this.numConnectionsThatContributed;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AbstractTypeSchema) {
			AbstractTypeSchema<?> other = (AbstractTypeSchema<?>) obj;
			return this.schema.equals(other.schema) && 
					this.numConnectionsThatContributed == other.numConnectionsThatContributed;
		} else {
			return false;
		}
	}


	@Override
	public String toString() {
		return "<" + this.numConnectionsThatContributed + "> : " + this.schema.toString();
	}
}