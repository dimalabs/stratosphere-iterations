package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class ImprovedAdjacencyListInputFormat extends TextInputFormat {

  private final AsciiStringView view = new AsciiStringView();
  private final PactLong vertexID = new PactLong();
  private final LongArrayView adjacentVertices = new LongArrayView();

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

    view.set(bytes, offset, numBytes);

    int numTokens = view.numTokens();
    adjacentVertices.allocate(numTokens - 1);

    try {
      for (int n = 0; n < numTokens; n++) {
        view.nextToken();

        if (n == 0) {
          vertexID.setValue(view.tokenAsLong());
        } else {
          adjacentVertices.setQuick(n - 1, view.tokenAsLong());
        }
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("Error parsing >>" + new String(bytes, offset, numBytes) + "<<", e);
    }

    target.clear();
    target.addField(vertexID);
    target.addField(adjacentVertices);

    return true;
  }
}

