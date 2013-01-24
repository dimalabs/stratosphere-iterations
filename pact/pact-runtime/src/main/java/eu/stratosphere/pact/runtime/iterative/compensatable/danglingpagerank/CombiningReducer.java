package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

import java.util.Iterator;

public class CombiningReducer extends ReduceStub {

  private final PactDouble agg = new PactDouble();

  @Override
  public void reduce(Iterator<PactRecord> partialRanks, Collector<PactRecord> out) throws Exception {

    double summedRank = 0;

    PactRecord curr = null;
    while (partialRanks.hasNext()) {
      curr = partialRanks.next();
      summedRank += curr.getField(1, PactDouble.class).getValue();
    }
    agg.setValue(summedRank);
    curr.setField(1, agg);
    out.collect(curr);
  }
}
