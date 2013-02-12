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
package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.generic.types.TypeComparator;


/**
 *
 */
public final class VertexWithAdjacencyListComparator implements TypeComparator<VertexWithAdjacencyList> {

	private long reference;
	
	@Override
	public int hash(VertexWithAdjacencyList record) {
		final long value = record.getVertexID();
		return 43 + (int) (value ^ value >>> 32);
	}

	@Override
	public void setReference(VertexWithAdjacencyList toCompare) {
		this.reference = toCompare.getVertexID();
	}

	@Override
	public boolean equalToReference(VertexWithAdjacencyList candidate) {
		return candidate.getVertexID() == this.reference;
	}

	@Override
	public int compareToReference(TypeComparator<VertexWithAdjacencyList> referencedComparator) {
		VertexWithAdjacencyListComparator comp = (VertexWithAdjacencyListComparator) referencedComparator;
		final long diff = comp.reference - this.reference;
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}

	@Override
	public int compare(DataInputView source1, DataInputView source2) throws IOException {
		final long diff = source1.readLong() - source2.readLong();
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 8;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 8;
	}

	@Override
	public void putNormalizedKey(VertexWithAdjacencyList record, byte[] target, int offset, int len) {
		final long value = record.getVertexID();
		
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

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(VertexWithAdjacencyList record, DataOutputView target) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readWithKeyDenormalization(VertexWithAdjacencyList record, DataInputView source) {
		throw new UnsupportedOperationException();
	}

	@Override
	public VertexWithAdjacencyListComparator duplicate() {
		return new VertexWithAdjacencyListComparator();
	}
}
