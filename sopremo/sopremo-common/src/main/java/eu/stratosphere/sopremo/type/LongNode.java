package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * This node represents a long value.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class LongNode extends AbstractNumericNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8594695207002513755L;

	private transient PactLong value;

	/**
	 * Initializes a LongNode which represents the given <code>long</code>. To create new LongNodes please
	 * use LongNode.valueOf(<code>long</code>) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public LongNode(final long value) {
		this.value = new PactLong(value);
	}

	/**
	 * Initializes LongNode.
	 */
	public LongNode() {
		this(0);
	}

	@Override
	public Long getJavaValue() {
		return this.value.getValue();
	}

	public void setValue(final long value) {
		this.value.setValue(value);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.value.write(out);
	}

	/**
	 * Creates a new instance of LongNode. This new instance represents the given value.
	 * 
	 * @param v
	 *        the value that should be represented by the new instance
	 * @return the newly created instance of LongNode
	 */
	public static LongNode valueOf(final long value) {
		return new LongNode(value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final LongNode other = (LongNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public int getIntValue() {
		return (int) this.value.getValue();
	}

	@Override
	public long getLongValue() {
		return this.value.getValue();
	}

	@Override
	public BigInteger getBigIntegerValue() {
		return BigInteger.valueOf(this.value.getValue());
	}

	@Override
	public BigDecimal getDecimalValue() {
		return BigDecimal.valueOf(this.value.getValue());
	}

	@Override
	public double getDoubleValue() {
		return Double.valueOf(this.value.getValue());
	}

	@Override
	public boolean isIntegralNumber() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.LongNode;
	}

	@Override
	public String getValueAsText() {
		return this.value.toString();
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append(this.value);
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeLong(this.value.getValue());
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		this.value = new PactLong(in.readLong());
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
		this.value.setValue(((LongNode) otherNode).getLongValue());
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return Long.signum(this.value.getValue() - ((LongNode) other).value.getValue());
	}

	@Override
	public void clear() {
		if (SopremoUtil.DEBUG)
			this.value.setValue(0);
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return this.value.getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		this.value.copyNormalizedKey(target, offset, len);
	}
}
