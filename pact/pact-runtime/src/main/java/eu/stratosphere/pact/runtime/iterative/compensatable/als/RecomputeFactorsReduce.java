package eu.stratosphere.pact.runtime.iterative.compensatable.als;

class RecomputeFactorsReduce {}
/*import com.google.common.collect.Lists;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;

import java.util.Iterator;
import java.util.List;
import org.apache.mahout.math.Vector;

public class RecomputeFactorsReduce extends ReduceStub {

  private PactRecord result = new PactRecord();

  private PactInteger userOrItemID = new PactInteger();
  private DoubleArray factors = new DoubleArray();
  private double[] factorValues = new double[NUM_FEATURES];

  private AlternatingLeastSquaresSolver solver = new AlternatingLeastSquaresSolver();

  private static final double LAMBDA = 0.085;
  private static final int NUM_FEATURES = 3;

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {

    List<Vector> featureVectors = Lists.newArrayList();
    List<Double> ratings = Lists.newArrayList();

    int id = -1;
    boolean first = true;

    while (records.hasNext()) {
      PactRecord record = records.next();

      if (first) {
        id = record.getField(0, PactInteger.class).getValue();
        first = false;
      }

      featureVectors.add(new DenseVector(record.getField(2, DoubleArray.class).values()));
      ratings.add(record.getField(1, PactDouble.class).getValue());
    }

    Vector ratingVector = new DenseVector(ratings.size());
    int index = 0;
    for (double rating : ratings) {
      ratingVector.setQuick(index++, rating);
    }

    Vector recomputedFactors = solver.solve(featureVectors, ratingVector, LAMBDA, NUM_FEATURES);

    for (int n = 0; n < NUM_FEATURES; n++) {
      factorValues[n] = recomputedFactors.getQuick(n);
    }

    userOrItemID.setValue(id);
    factors.set(factorValues);
    result.setField(0, userOrItemID);
    result.setField(1, factors);
    out.collect(result);
  }
}      */
