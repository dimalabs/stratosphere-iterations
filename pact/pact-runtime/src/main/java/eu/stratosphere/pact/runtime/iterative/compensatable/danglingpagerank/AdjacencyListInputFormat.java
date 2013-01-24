package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class AdjacencyListInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  private final PactLong vertexID = new PactLong();
  private final PactLongArray adjacentVertices = new PactLongArray();

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    vertexID.setValue(Long.parseLong(parts[0]));


    int numEntries = (parts.length - 1);
    long[] indexes = new long[numEntries];

    for (int n = 0; n < numEntries; n++) {
      indexes[n] = Long.parseLong(parts[n + 1]);
    }

    adjacentVertices.set(indexes);

    target.clear();
    target.addField(vertexID);
    target.addField(adjacentVertices);

    return true;
  }
}

//public class AdjacencyListInputFormat extends TextInputFormat {
//
//  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");
//
//  @Override
//  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
//    String str = new String(bytes, offset, numBytes);
//    String[] parts = SEPARATOR.split(str);
//
//    target.clear();
//    target.addField(new PactLong(Long.parseLong(parts[0])));
//
//    int numEntries = (parts.length - 1);
//    long[] indexes = new long[numEntries];
//
//    for (int n = 0; n < numEntries; n++) {
//      indexes[n] = Long.parseLong(parts[n + 1]);
//    }
//
//    target.addField(new PactLongArray(indexes));
//
//    return true;
//  }
//}
