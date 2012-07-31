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

package eu.stratosphere.nephele.io.channels.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author Stephan Ewen
 */
public class ByteSubArrayType implements SerializationTestType
{
	private static final int MAX_LEN = 512;

	private final byte[] data;
	private int len;
	
	public ByteSubArrayType()
	{
		this.data = new byte[MAX_LEN];
		this.len = 0;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.serialization.SerializationTestType#getRandom(java.util.Random)
	 */
	@Override
	public ByteSubArrayType getRandom(Random rnd)
	{
		final int len = rnd.nextInt(MAX_LEN) + 1;
		final ByteSubArrayType t = new ByteSubArrayType();
		t.len = len;
		
		final byte[] data = t.data;
		for (int i = 0; i < len; i++) {
			data[i] = (byte) rnd.nextInt(256);
		}
		
		return t;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.len);
		out.write(this.data, 0, this.len);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		this.len = in.readInt();
		in.readFully(this.data, 0, this.len);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final byte[] copy = new byte[this.len];
		System.arraycopy(this.data, 0, copy, 0, this.len);
		return Arrays.hashCode(copy);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof ByteSubArrayType) {
			ByteSubArrayType other = (ByteSubArrayType) obj;
			if (this.len == other.len) {
				for (int i = 0; i < this.len; i++) {
					if (this.data[i] != other.data[i]) {
						return false;
					}
				}
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
}
