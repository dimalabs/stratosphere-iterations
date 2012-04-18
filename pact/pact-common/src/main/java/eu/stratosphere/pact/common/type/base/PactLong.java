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
import eu.stratosphere.pact.common.type.NormalizableKey;

/**
 * Long base type for PACT programs that implements the Key interface.
 * PactLong encapsulates a Java primitive long.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PactLong implements Key, NormalizableKey
{

	private long value;

	/**
	 * Initializes the encapsulated long with 0.
	 */
	public PactLong() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated long with the specified value. 
	 * 
	 * @param value Initial value of the encapsulated long.
	 */
	public PactLong(final long value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated long.
	 * 
	 * @return The value of the encapsulated long.
	 */
	public long getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated long to the specified value.
	 * 
	 * @param value
	 *        The new value of the encapsulated long.
	 */
	public void setValue(final long value) {
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
		this.value = in.readLong();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeLong(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactLong))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");

		final long other = ((PactLong) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return 43 + (int) (this.value ^ this.value >>> 32);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj)
	{
		if (obj != null & obj instanceof PactLong) {
			return this.value == ((PactLong) obj).value;  
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#getNormalizedKeyLen()
	 */
	@Override
	public int getMaxNormalizedKeyLen()
	{
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#copyNormalizedKey(byte[], int, int)
	 */
	@Override
	public void copyNormalizedKey(byte[] target, int offset, int len)
	{
		if (len == 8) {
			// default case, full normalized key
			long highByte = ((value >>> 56) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) (value >>> 48);
			target[offset + 2] = (byte) (value >>> 40);
			target[offset + 3] = (byte) (value >>> 32);
			target[offset + 4] = (byte) (value >>> 24);
			target[offset + 5] = (byte) (value >>> 16);
			target[offset + 6] = (byte) (value >>>  8);
			target[offset + 7] = (byte) (value       );
		}
		else if (len <= 0) {
		}
		else if (len < 8) {
			long highByte = ((value >>> 56) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset] = (byte) highByte;
			len--;
			for (int i = 1; len > 0; len--, i++) {
				target[offset + i] = (byte) (value >>> ((7-i)<<3));
			}
		}
		else {
			long highByte = ((value >>> 56) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) (value >>> 48);
			target[offset + 2] = (byte) (value >>> 40);
			target[offset + 3] = (byte) (value >>> 32);
			target[offset + 4] = (byte) (value >>> 24);
			target[offset + 5] = (byte) (value >>> 16);
			target[offset + 6] = (byte) (value >>>  8);
			target[offset + 7] = (byte) (value       );
			for (int i = 8; i < len; i++) {
				target[offset + i] = 0;
			}
		}
	}
}
