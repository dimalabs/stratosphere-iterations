package eu.stratosphere.pact.runtime.iterative.types;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;


/**
 * 
 */
public class TransitiveClosureEntryAccessors implements TypeAccessorsV2<TransitiveClosureEntry>
{
	private int referenceKey;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createInstance()
	 */
	@Override
	public TransitiveClosureEntry createInstance()
	{
		return new TransitiveClosureEntry();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createCopy(java.lang.Object)
	 */
	@Override
	public TransitiveClosureEntry createCopy(TransitiveClosureEntry from)
	{
		return new TransitiveClosureEntry(
			from.getVid(), from.getCid(),
			Arrays.copyOf(from.getNeighbors(), from.getNumNeighbors()));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(TransitiveClosureEntry from, TransitiveClosureEntry to)
	{
		to.setVid(from.getVid());
		to.setCid(from.getCid());
		to.setNumNeighbors(from.getNumNeighbors());
		
		if (to.getNeighbors().length < from.getNumNeighbors()) {
			System.arraycopy(from.getNeighbors(), 0, to.getNeighbors(), 0, from.getNumNeighbors());
		} else {
			to.setNeighbors(Arrays.copyOf(from.getNeighbors(), from.getNumNeighbors()));
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength()
	{
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public long serialize(TransitiveClosureEntry record, DataOutputViewV2 target) throws IOException
	{
		target.writeInt(record.getVid());
		target.writeInt(record.getCid());
		
		final int[] n = record.getNeighbors();
		final int num = record.getNumNeighbors();
		
		target.write(num);
		for (int i = 0; i < num; i++) {
			target.writeInt(n[i]);
		}
		
		return (num * 4) + 12;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public void deserialize(TransitiveClosureEntry target, DataInputViewV2 source) throws IOException
	{
		target.setVid(source.readInt());
		target.setCid(source.readInt());
		
		final int num = source.readInt();
		target.setNumNeighbors(num);
		
		final int[] n;
		if (target.getNeighbors().length >= num) {
			n = target.getNeighbors();
		} else {
			n = new int[num];
			target.setNeighbors(n);
		}
		for (int i = 0; i < num; i++) {
			n[i] = source.readInt();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException
	{
		// copy vid, cid
		for (int i = 0; i < 8; i++) {
			target.writeByte(source.readUnsignedByte());
		}
		
		int num = source.readInt();
		target.writeInt(num);
		for (int i = 4 * num; i > 0; --i) {
			target.writeByte(source.readUnsignedByte());
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#hash(java.lang.Object)
	 */
	@Override
	public int hash(TransitiveClosureEntry object) {
		return object.getVid();
	}
	
	@Override
	public void setReferenceForEquality(TransitiveClosureEntry reference) {
		this.referenceKey = reference.getVid();
	}
	
	@Override
	public boolean equalToReference(TransitiveClosureEntry candidate) {
		return candidate.getVid() == this.referenceKey;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
	 */
	@Override
	public int compare(TransitiveClosureEntry first, TransitiveClosureEntry second, Comparator<Key> comparator) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public int compare(DataInputViewV2 source1, DataInputViewV2 source2) throws IOException {
		return source1.readInt() - source2.readInt();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#supportsNormalizedKey()
	 */
	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getNormalizeKeyLen()
	 */
	@Override
	public int getNormalizeKeyLen() {
		return 4;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#isNormalizedKeyPrefixOnly(int)
	 */
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
	@Override
	public void putNormalizedKey(TransitiveClosureEntry record, byte[] target, int offset, int numBytes)
	{
		final int value = record.getVid();
		
		if (numBytes == 4) {
			// default case, full normalized key
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) ((value >>> 16) & 0xff);
			target[offset + 2] = (byte) ((value >>>  8) & 0xff);
			target[offset + 3] = (byte) ((value       ) & 0xff);
		}
		else if (numBytes <= 0) {
		}
		else if (numBytes < 4) {
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			numBytes--;
			for (int i = 1; numBytes > 0; numBytes--, i++) {
				target[offset + i] = (byte) ((value >>> ((3-i)<<3)) & 0xff);
			}
		}
		else {
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) ((value >>> 16) & 0xff);
			target[offset + 2] = (byte) ((value >>>  8) & 0xff);
			target[offset + 3] = (byte) ((value       ) & 0xff);
			for (int i = 4; i < numBytes; i++) {
				target[offset + i] = 0;
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#duplicate()
	 */
	@Override
	public TransitiveClosureEntryAccessors duplicate() {
		return new TransitiveClosureEntryAccessors();
	}
}
