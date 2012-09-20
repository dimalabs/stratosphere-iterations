package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.Iterator;

public class MinimumComponentIDReduce extends ReduceStub {

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {

    PactRecord first = records.next();
    long minimumComponentID = first.getField(1, PactLong.class).getValue();

    long vertexID = first.getField(0, PactLong.class).getValue();

    while (records.hasNext()) {
      long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
      if (candidateComponentID < minimumComponentID) {
        minimumComponentID = candidateComponentID;
      }
    }

    PactRecord result = new PactRecord();
    result.setField(0, new PactLong(vertexID));
    result.setField(1, new PactLong(minimumComponentID));

    System.out.println("Candidate component of vertex " + vertexID + " is " + minimumComponentID);

    out.collect(result);
  }
}