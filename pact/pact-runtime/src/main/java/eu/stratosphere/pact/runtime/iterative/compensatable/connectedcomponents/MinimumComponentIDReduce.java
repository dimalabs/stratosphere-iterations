package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class MinimumComponentIDReduce extends ReduceStub {

  private PactRecord result = new PactRecord();

  private int workerIndex;
  private int currentIteration;

  private int failingIteration;
  private Set<Integer> failingWorkers;
  private double messageLoss;

  private Random random;

  @Override
  public void open(Configuration parameters) throws Exception {
    workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
    currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
    failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
    failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
    messageLoss = ConfigUtils.asDouble("compensation.messageLoss", parameters);

    random = new Random();
  }
  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {

    if (currentIteration == failingIteration && failingWorkers.contains(workerIndex) &&
        random.nextDouble() <= messageLoss) {
      return;
    }

    PactRecord first = records.next();
    long minimumComponentID = first.getField(1, PactLong.class).getValue();

    long vertexID = first.getField(0, PactLong.class).getValue();

    while (records.hasNext()) {
      long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
      if (candidateComponentID < minimumComponentID) {
        minimumComponentID = candidateComponentID;
      }
    }

    result.setField(0, new PactLong(vertexID));
    result.setField(1, new PactLong(minimumComponentID));

//    System.out.println("-------------- Candidate component of vertex " + vertexID + " is " + minimumComponentID);

    out.collect(result);
  }
}
