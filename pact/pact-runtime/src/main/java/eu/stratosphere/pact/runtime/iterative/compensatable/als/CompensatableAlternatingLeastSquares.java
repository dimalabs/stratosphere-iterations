package eu.stratosphere.pact.runtime.iterative.compensatable.als;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.CompensatableDotProductCoGroup;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.CompensatableDotProductMatch;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.CompensatingMap;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.DanglingPageGenerateRankInputFormat;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.DanglingPageRankHdfsCheckpointWriter;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.DiffL1NormConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.driver.RepeatableHashjoinMatchDriverWithCachedBuildside;
import eu.stratosphere.pact.runtime.iterative.driver.SortingTempDriver;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.PageWithRankOutFormat;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.CoGroupDriver;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.TempDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class CompensatableAlternatingLeastSquares {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String initialItemFactorsInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/als/initialItemFactors";
    String ratingsInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/als/ratings";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";
    int memoryPerTask = 25;
    int memoryForMatch = memoryPerTask;
    int numIterations = 25;
    long numVertices = 5;
    long numDanglingVertices = 1;

    String failingWorkers = "1";
    int failingIteration = 26;
    double messageLoss = 0.75;

    String checkpointPath = null;
    //String checkpointPath = "file:///tmp/checkpoints";

//    if (args.length >= 14) {
//      degreeOfParallelism = Integer.parseInt(args[0]);
//      numSubTasksPerInstance = Integer.parseInt(args[1]);
//      initialItemFactorsInputPath = args[2];
//      ratingsInputPath = args[3];
//      outputPath = args[4];
//      confPath = args[5];
//      memoryPerTask = Integer.parseInt(args[6]);
//      memoryForMatch = Integer.parseInt(args[7]);
//      numIterations = Integer.parseInt(args[8]);
//     numVertices = Long.parseLong(args[9]);
//      numDanglingVertices = Long.parseLong(args[10]);
//      failingWorkers = args[11];
//      failingIteration = Integer.parseInt(args[12]);
//      messageLoss = Double.parseDouble(args[13]);
//
//      if (args.length == 15) {
//        checkpointPath = args[14];
//      }
//    }

    JobGraph jobGraph = new JobGraph("CompensatableDanglingPageRank");

    JobInputVertex initialFactorsInput = JobGraphUtils.createInput(InitialFactorsInputFormat.class,
        initialItemFactorsInputPath, "InitialItemFactorsInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig initialFactorsInputConfig = new TaskConfig(initialFactorsInput.getConfiguration());
    initialFactorsInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(initialFactorsInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });


    JobInputVertex ratingsInput = JobGraphUtils.createInput(RatingsInputFormat.class, ratingsInputPath, "RatingsInput",
        jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig ratingsInputConfig = new TaskConfig(ratingsInput.getConfiguration());
    ratingsInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(ratingsInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(ratingsInputConfig.getConfigForOutputParameters(1),
        new int[] { 1 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    headConfig.setStubClass(UserFactorsPerItemMatch.class);
    headConfig.setMemorySize((long) (memoryForMatch * 1.25 * JobGraphUtils.MEGABYTE));
    headConfig.setBackChannelMemoryFraction(0.2f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex recomputeUserFactorsReduce = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "RecomputeUserFactorsReduce", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig recomputeUserFactorsConfig = new TaskConfig(recomputeUserFactorsReduce.getConfiguration());
    recomputeUserFactorsConfig.setDriver(ReduceDriver.class);
    recomputeUserFactorsConfig.setStubClass(RecomputeFactorsReduce.class);
    recomputeUserFactorsConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(recomputeUserFactorsConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    recomputeUserFactorsConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    recomputeUserFactorsConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(recomputeUserFactorsConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex itemFactorsPerUserMatch = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "ItemFactorsPerUserMatch", jobGraph,  degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig itemFactorsPerUserConfig = new TaskConfig(itemFactorsPerUserMatch.getConfiguration());
    itemFactorsPerUserConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    itemFactorsPerUserConfig.setStubClass(ItemFactorsPerUserMatch.class);
    itemFactorsPerUserConfig.setMemorySize(memoryForMatch * JobGraphUtils.MEGABYTE);
    itemFactorsPerUserConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(itemFactorsPerUserConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(itemFactorsPerUserConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(itemFactorsPerUserConfig.getConfigForInputParameters(1),
        new int[] { 1 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "RecomputeItemFactorsReduce", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(RecomputeFactorsReduce.class);
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    tailConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    tailConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForOutputParameters(0),
        new int[]{ 0 }, new Class[]{ PactInteger.class }, new boolean[] { true });

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(numIterations);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
        numSubTasksPerInstance);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism,
        numSubTasksPerInstance);

//    //TODO implicit order should be documented/configured somehow
//    JobGraphUtils.connect(initialFactorsInput, sortedPartitionedPageRank, ChannelType.NETWORK,
//        DistributionPattern.BIPARTITE, ShipStrategy.ShipStrategyType.PARTITION_HASH);
//    JobGraphUtils.connect(sortedPartitionedPageRank, itemFactorsPerUserMatch, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
//        ShipStrategy.ShipStrategyType.FORWARD);
//
//    JobGraphUtils.connect(itemFactorsPerUserMatch, recomputeUserFactorsReduce, ChannelType.NETWORK, DistributionPattern.POINTWISE,
//        ShipStrategy.ShipStrategyType.FORWARD);
//    JobGraphUtils.connect(ratingsInput, recomputeUserFactorsReduce, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
//        ShipStrategy.ShipStrategyType.PARTITION_HASH);
//    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
//
//    JobGraphUtils.connect(itemFactorsPerUserMatch, tempIntermediate, ChannelType.NETWORK, DistributionPattern.POINTWISE,
//        ShipStrategy.ShipStrategyType.FORWARD);
//    tempIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
//
//    JobGraphUtils.connect(tempIntermediate, tail, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
//        ShipStrategy.ShipStrategyType.FORWARD);
//    JobGraphUtils.connect(recomputeUserFactorsReduce, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
//        ShipStrategy.ShipStrategyType.PARTITION_HASH);
//    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
//    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);


    JobGraphUtils.connect(itemFactorsPerUserMatch, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(itemFactorsPerUserMatch, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);


    fakeTailOutput.setVertexToShareInstancesWith(tail);
    tail.setVertexToShareInstancesWith(itemFactorsPerUserMatch);
    initialFactorsInput.setVertexToShareInstancesWith(itemFactorsPerUserMatch);
    ratingsInput.setVertexToShareInstancesWith(itemFactorsPerUserMatch);
    recomputeUserFactorsReduce.setVertexToShareInstancesWith(itemFactorsPerUserMatch);
    output.setVertexToShareInstancesWith(itemFactorsPerUserMatch);
    sync.setVertexToShareInstancesWith(itemFactorsPerUserMatch);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
