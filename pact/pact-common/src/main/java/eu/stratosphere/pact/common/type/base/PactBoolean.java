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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

/**
 * Boolean base type for PACT programs that implements the Key interface.
 * PactBoolean encapsulates a Java primitive boolean.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 */
public class PactBoolean implements Key
{
	private boolean value;

	/**
	 * Initializes the encapsulated boolean with false.
	 */
	public PactBoolean() {
		this.value = false;
	}

	/**
	 * Initializes the encapsulated boolean with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated boolean.
	 */
	public PactBoolean(final boolean value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated boolean.
	 * 
	 * @return the value of the encapsulated boolean.
	 */
	public boolean getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated boolean to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated boolean.
	 */
	public void setValue(final boolean value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readBoolean();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeBoolean(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactBoolean))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactBoolean!");

		final boolean other = ((PactBoolean) o).value;

		// Sort false before true
		return this.value == other ? 0 : this.value ? 1 : -1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.value ? 17 : 18;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof PactBoolean) {
			return ((PactBoolean) obj).value == this.value;
		}
		return false;
	}
}
