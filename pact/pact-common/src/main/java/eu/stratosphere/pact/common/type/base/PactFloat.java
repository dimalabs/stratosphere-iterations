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
 * Float base type for PACT programs that implements the Key interface.
 * PactFloat encapsulates a Java primitive float.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 */
public class PactFloat implements Key {

	private float value;

	/**
	 * Initializes the encapsulated float with 0.0.
	 */
	public PactFloat() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated float with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated float.
	 */
	public PactFloat(final float value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated primitive float.
	 * 
	 * @return the value of the encapsulated primitive float.
	 */
	public float getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated primitive float.
	 * 
	 * @param value
	 *        the new value of the encapsulated primitive float.
	 */
	public void setValue(final float value) {
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

	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readFloat();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeFloat(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactFloat))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactFloat!");

		final float other = ((PactFloat) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Float.floatToIntBits(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof PactFloat) {
			return ((PactFloat) obj).value == this.value;
		}
		return false;
	}
}
