package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class NeighborComponentIDToWorksetMatch extends MatchStub {

  @Override
  public void match(PactRecord edge, PactRecord vertexWithComponent, Collector<PactRecord> out)
      throws Exception {

    PactRecord result = new PactRecord();

    result.setField(0, edge.getField(1, PactLong.class));
    result.setField(1, vertexWithComponent.getField(1, PactLong.class));

    long sourceVertexID = edge.getField(0, PactLong.class).getValue();
    long targetVertexID = edge.getField(1, PactLong.class).getValue();
    long candidateComponentID = vertexWithComponent.getField(1, PactLong.class).getValue();

    System.out.println("Sending component " + candidateComponentID +" of vertex " + sourceVertexID +  " to " + targetVertexID);

    out.collect(result);
  }
}