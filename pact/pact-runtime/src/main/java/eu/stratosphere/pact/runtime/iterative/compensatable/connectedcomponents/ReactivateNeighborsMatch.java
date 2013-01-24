package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class ReactivateNeighborsMatch extends MatchStub {

  public static final long REACTIVATION_SIGNAL = -1;

  private PactRecord result = new PactRecord();
  private PactLong neighbor = new PactLong();
  private PactLong reactivationMessage = new PactLong(REACTIVATION_SIGNAL);

  @Override
  public void match(PactRecord vertexWithComponent, PactRecord adjacencyList, Collector<PactRecord> out)
      throws Exception {

    result.setField(1, reactivationMessage);

    long[] neighborIDs = adjacencyList.getField(1, AdjacencyList.class).neighborIDs();

    for (long neighborID : neighborIDs) {
      neighbor.setValue(neighborID);
      result.setField(0, neighbor);
      out.collect(result);
    }

  }
}
