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
 * Short base type for PACT programs that implements the Key interface.
 * PactShort encapsulates a Java primitive short.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 */
public class PactShort implements Key
{
	private short value;

	/**
	 * Initializes the encapsulated short with 0.
	 */
	public PactShort() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated short with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated short.
	 */
	public PactShort(final short value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated short.
	 * 
	 * @return the value of the encapsulated short.
	 */
	public short getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated short to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated short.
	 */
	public void setValue(final short value) {
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
		this.value = in.readShort();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeShort(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactShort))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactShort!");

		final short other = ((PactShort) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof PactShort) {
			return ((PactShort) obj).value == this.value;
		}
		return false;
	}
}
