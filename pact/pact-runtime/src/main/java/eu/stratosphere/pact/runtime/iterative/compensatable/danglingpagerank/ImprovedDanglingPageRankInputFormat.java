package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;

public class ImprovedDanglingPageRankInputFormat extends TextInputFormat {

  private PactLong vertexID = new PactLong();
  private PactDouble initialRank;
  private BooleanValue isDangling = new BooleanValue();

  private AsciiStringView view = new AsciiStringView();

  @Override
  public void configure(Configuration parameters) {
    long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
    initialRank = new PactDouble(1 / (double) numVertices);
    super.configure(parameters);
  }

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

    view.set(bytes, offset, numBytes);

    try {
      view.nextToken();
      vertexID.setValue(view.tokenAsLong());

      isDangling.set(view.nextToken() && view.tokenAsLong() == 1l);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing >>" + new String(bytes, offset, numBytes) + "<<", e);
    }

    target.clear();
    target.addField(vertexID);
    target.addField(initialRank);
    target.addField(isDangling);

    return true;
  }
}
