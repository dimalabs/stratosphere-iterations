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

package eu.stratosphere.pact.runtime.io;

import java.io.IOException;
import java.io.UTFDataFormatException;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * The base class for all output views that are backed by multiple pages.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class AbstractPagedOutputView implements DataOutputView
{
	private MemorySegment currentSegment;			// the current memory segment to write to
	
	private byte[] utfBuffer;						// the reusable array for UTF encodings
	
	private final int segmentSize;					// the size of the memory segments
	
	private final int headerLength;					// the number of bytes to skip at the beginning of each segment
	
	private int positionInSegment;					// the offset in the current segment
	
	
	// --------------------------------------------------------------------------------------------
	//                                    Constructors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param segmentSize The size of the memory segments.
	 * @param headerLength The number of bytes to skip at the beginning of each header.
	 */
	protected AbstractPagedOutputView(final MemorySegment initialSegment,
			final int segmentSize, final int headerLength) throws IOException
	{
		this.segmentSize = segmentSize;
		this.headerLength = headerLength;
		
		this.currentSegment = initialSegment;
		this.positionInSegment = headerLength;
	}
	
	/**
	 * @param segmentSize The size of the memory segments.
	 */
	protected AbstractPagedOutputView(final int segmentSize, final int headerLength)
	{
		this.segmentSize = segmentSize;
		this.headerLength = headerLength;
	}
	

	// --------------------------------------------------------------------------------------------
	//                                  Page Management
	// --------------------------------------------------------------------------------------------
	
	protected abstract MemorySegment nextSegment(MemorySegment current) throws IOException;
	
	protected void advance() throws IOException
	{
		this.currentSegment = nextSegment(this.currentSegment);
		this.positionInSegment = this.headerLength;
	}
	
	protected void clear() {
		this.currentSegment = null;
		this.positionInSegment = this.headerLength;
	}
	
	public MemorySegment getCurrentSegment() {
		return this.currentSegment;
	}
	
	public int getCurrentPositionInSegment() {
		return this.positionInSegment;
	}
	
	// --------------------------------------------------------------------------------------------
	//                               Data Output Specific methods
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(int)
	 */
	@Override
	public void write(int b) throws IOException
	{
		writeByte(b);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(byte[])
	 */
	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(byte[], int, int)
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException
	{
		int remaining = this.segmentSize - this.positionInSegment;
		if (remaining >= len) {
			this.currentSegment.put(this.positionInSegment, b, off, len);
			this.positionInSegment += len;
		}
		else {
			if (remaining == 0) {
				advance();
				remaining = this.segmentSize - this.positionInSegment;
			}
			while (true) {
				int toPut = Math.min(remaining, len);
				this.currentSegment.put(this.positionInSegment, b, off, toPut);
				off += toPut;
				len -= toPut;
				
				if (len > 0) {
					this.positionInSegment = this.segmentSize;
					advance();
					remaining = this.segmentSize - this.positionInSegment;	
				}
				else {
					this.positionInSegment += toPut;
					break;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeBoolean(boolean)
	 */
	@Override
	public void writeBoolean(boolean v) throws IOException
	{
		writeByte(v ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeByte(int)
	 */
	@Override
	public void writeByte(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize) {
			this.currentSegment.put(this.positionInSegment++, (byte) v);
		}
		else {
			advance();
			this.currentSegment.put(this.positionInSegment++, (byte) v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeShort(int)
	 */
	@Override
	public void writeShort(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putShort(this.positionInSegment, (short) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			this.currentSegment.putShort(this.positionInSegment, (short) v);
			this.positionInSegment += 2;
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeChar(int)
	 */
	@Override
	public void writeChar(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putChar(this.positionInSegment, (char) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			this.currentSegment.putChar(this.positionInSegment, (char) v);
			this.positionInSegment += 2;
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeInt(int)
	 */
	@Override
	public void writeInt(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 3) {
			this.currentSegment.putInt(this.positionInSegment, v);
			this.positionInSegment += 4;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			this.currentSegment.putInt(this.positionInSegment, v);
			this.positionInSegment += 4;
		}
		else {
			writeByte(v >> 24);
			writeByte(v >> 16);
			writeByte(v >>  8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeLong(long)
	 */
	@Override
	public void writeLong(long v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 7) {
			this.currentSegment.putLong(this.positionInSegment, v);
			this.positionInSegment += 8;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			this.currentSegment.putLong(this.positionInSegment, v);
			this.positionInSegment += 8;
		}
		else {
			writeByte((int) (v >> 56));
			writeByte((int) (v >> 48));
			writeByte((int) (v >> 40));
			writeByte((int) (v >> 32));
			writeByte((int) (v >> 24));
			writeByte((int) (v >> 16));
			writeByte((int) (v >>  8));
			writeByte((int) v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeFloat(float)
	 */
	@Override
	public void writeFloat(float v) throws IOException
	{
		writeInt(Float.floatToIntBits(v));
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeDouble(double)
	 */
	@Override
	public void writeDouble(double v) throws IOException
	{
		writeLong(Double.doubleToLongBits(v));
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeBytes(java.lang.String)
	 */
	@Override
	public void writeBytes(String s) throws IOException
	{
		for (int i = 0; i < s.length(); i++) {
			writeByte(s.charAt(i));
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeChars(java.lang.String)
	 */
	@Override
	public void writeChars(String s) throws IOException
	{
		for (int i = 0; i < s.length(); i++) {
			writeChar(s.charAt(i));
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeUTF(java.lang.String)
	 */
	@Override
	public void writeUTF(String str) throws IOException
	{
		int strlen = str.length();
		int utflen = 0;
		int c, count = 0;

		/* use charAt instead of copying String to char array */
		for (int i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			} else if (c > 0x07FF) {
				utflen += 3;
			} else {
				utflen += 2;
			}
		}

		if (utflen > 65535)
			throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");

		if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
			this.utfBuffer = new byte[utflen + 2];
		}
		final byte[] bytearr = this.utfBuffer;

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

		int i = 0;
		for (i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if (!((c >= 0x0001) && (c <= 0x007F)))
				break;
			bytearr[count++] = (byte) c;
		}

		for (; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				bytearr[count++] = (byte) c;

			} else if (c > 0x07FF) {
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
			} else {
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
			}
		}

		write(bytearr, 0, utflen + 2);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#skip(int)
	 */
	@Override
	public DataOutputView skip(int numBytes) throws IOException
	{
		while (numBytes > 0) {
			final int remaining = this.segmentSize - this.positionInSegment;
			if (numBytes <= remaining) {
				this.positionInSegment += numBytes;
				return this;
			}
			
			advance();
			numBytes -= remaining;
		}
		return this;
	}
}