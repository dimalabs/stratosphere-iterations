package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactLongArray implements Value
{
	private static final long[] EMPTY = new long[0];
	
	private StringBuilder bld;

	private long[] values;
	private int num;
	
	public PactLongArray()
	{
		this.values = EMPTY;
	}
	
	
	public void clear() {
		this.num = 0;
	}
	
	public void addLong(long val) {
		if (this.num >= this.values.length) {
			long[] nv = new long[Math.max(this.values.length * 2, 64)];
			System.arraycopy(this.values, 0, nv, 0, this.values.length);
			this.values = nv;
		}
		
		this.values[num++] = val;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.num);
		for (int i = 0; i < this.num; i++) {
			out.writeLong(this.values[i]);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.num = in.readInt();
		if (this.values.length < this.num) {
			this.values = new long[this.num];
		}
		
		for (int i = 0; i < this.num; i++) {
			this.values[i] = in.readLong();
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (this.bld == null) {
			this.bld = new StringBuilder();
		}
		
		StringBuilder bld = this.bld;
		bld.setLength(0);
		
		for (int i = 0; i < this.num; i++) {
			bld.append(this.values[i]);
			bld.append(',');
		}
		
		// truncate last comma
		if (bld.length() > 0) {
			bld.setLength(bld.length() - 1);
		}
		
		return bld.toString();
	}
}