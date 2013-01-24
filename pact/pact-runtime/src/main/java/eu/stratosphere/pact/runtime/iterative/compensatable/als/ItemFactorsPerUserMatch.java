package eu.stratosphere.pact.runtime.iterative.compensatable.als;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ItemFactorsPerUserMatch extends MatchStub {

  private PactRecord result = new PactRecord();

  @Override
  public void match(PactRecord itemFactors, PactRecord preference, Collector<PactRecord> out) throws Exception {

    result.setField(0, preference.getField(0, PactInteger.class));
    result.setField(1, preference.getField(1, PactInteger.class));
    result.setField(1, preference.getField(2, PactDouble.class));
    result.setField(2, itemFactors.getField(1, DoubleArray.class));

    out.collect(result);
  }
}
