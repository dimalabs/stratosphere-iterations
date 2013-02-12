package eu.stratosphere.pact.example.iterative;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.example.datamining.KMeansIteration;
import eu.stratosphere.pact.generic.contract.BulkIteration;

/**
 *
 */
public class IterativeKMeans extends KMeansIteration
{
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String dataPointInput = (args.length > 1 ? args[1] : "");
		final String clusterInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for cluster center input
		FileDataSource initialClusterPoints = new FileDataSource(PointInFormat.class, clusterInput, "Centers");
		initialClusterPoints.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		initialClusterPoints.setDegreeOfParallelism(1);
		initialClusterPoints.getCompilerHints().setUniqueField(new FieldSet(0));
		
		BulkIteration iteration = new BulkIteration("K-Means Loop");
		iteration.setInput(initialClusterPoints);
		iteration.setNumberOfIterations(numIterations);
		
		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(PointInFormat.class, dataPointInput, "Data Points");
		PointInFormat.configureDelimitedFormat(dataPoints).recordDelimiter('\n');
		dataPoints.getCompilerHints().setUniqueField(new FieldSet(0));

		// create CrossContract for distance computation
		CrossContract computeDistance = CrossContract.builder(ComputeDistance.class)
				.input1(dataPoints)
				.input2(iteration.getPartialSolution())
				.name("Compute Distances")
				.build();
		computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract findNearestClusterCenters = ReduceContract.builder(FindNearestCenter.class, PactInteger.class, 0)
				.input(computeDistance)
				.name("Find Nearest Centers")
				.build();
		findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for computing new cluster positions
		ReduceContract recomputeClusterCenter = ReduceContract.builder(RecomputeClusterCenter.class, PactInteger.class, 0)
				.input(findNearestClusterCenters)
				.name("Recompute Center Positions")
				.build();
		recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);
		iteration.setNextPartialSolution(recomputeClusterCenter);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink finalResult = new FileDataSink(PointOutFormat.class, output, iteration, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(finalResult, "Iterative KMeans");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.example.datamining.KMeansIteration#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: <noSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
}
