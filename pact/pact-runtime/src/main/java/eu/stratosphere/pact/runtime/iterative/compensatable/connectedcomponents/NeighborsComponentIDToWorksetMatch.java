package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;

import java.util.Random;
import java.util.Set;

public class NeighborsComponentIDToWorksetMatch extends MatchStub {

  private PactRecord result = new PactRecord();
  private PactLong neighbor = new PactLong();

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
  public void match(PactRecord vertexWithComponent, PactRecord adjacencyList, Collector<PactRecord> out)
      throws Exception {

    if (currentIteration == failingIteration && failingWorkers.contains(workerIndex) &&
        random.nextDouble() <= messageLoss) {
      return;
    }

    result.setField(1, vertexWithComponent.getField(1, PactLong.class));

    long[] neighborIDs = adjacencyList.getField(1, AdjacencyList.class).neighborIDs();

    for (long neighborID : neighborIDs) {
      neighbor.setValue(neighborID);
      result.setField(0, neighbor);
      out.collect(result);
    }

  }
}
