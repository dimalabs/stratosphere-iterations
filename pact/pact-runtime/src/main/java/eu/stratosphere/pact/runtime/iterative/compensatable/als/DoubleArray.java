package eu.stratosphere.pact.runtime.iterative.compensatable.als;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleArray implements Value {

  private double[] values;

  public DoubleArray() {}

  public DoubleArray(double[] values) {
    this.values = values;
  }

  public double[] values() {
    return values;
  }

  public void set(double[] values) {
    this.values = values;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);
    for (int n = 0; n < values.length; n++) {
      out.writeDouble(values[n]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int length = in.readInt();
    values = new double[length];
    for (int n = 0; n < length; n++) {
      values[n] = in.readDouble();
    }
  }
}
