package eu.stratosphere.pact.runtime.iterative.compensatable.als;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.util.regex.Pattern;

public class InitialFactorsInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactInteger(Integer.parseInt(parts[0])));

    int numEntries = (parts.length - 1);
    double[] values = new double[numEntries];

    for (int n = 0; n < numEntries; n++) {
      values[n] = Double.parseDouble(parts[n + 1]);
    }

    target.addField(new DoubleArray(values));

    return true;
  }
}