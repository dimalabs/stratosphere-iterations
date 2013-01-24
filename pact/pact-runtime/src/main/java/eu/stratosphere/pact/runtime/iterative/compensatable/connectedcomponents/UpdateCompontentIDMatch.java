package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;

import java.util.Random;
import java.util.Set;


public class UpdateCompontentIDMatch extends MatchStub {

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
  public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent,
      Collector<PactRecord> out) throws Exception {

    long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
    long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();

    long vid = currentVertexWithComponent.getField(0, PactLong.class).getValue();

    if (candidateComponentID == ReactivateNeighborsMatch.REACTIVATION_SIGNAL) {
      System.out.println("REACTIVATING " + vid);
      return;
    }

    if (candidateComponentID < currentComponentID) {


      System.out.println("SETTING " + vid +" FROM " + currentComponentID + " TO " + candidateComponentID);

      out.collect(newVertexWithComponent);
    }
  }

}
