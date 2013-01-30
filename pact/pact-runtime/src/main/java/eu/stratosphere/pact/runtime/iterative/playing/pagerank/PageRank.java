/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.driver.RepeatableHashjoinMatchDriverWithCachedBuildside;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

@Deprecated
public class PageRank {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String pageWithRankInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/pageWithRank";
    String transitionMatrixInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/transitionMatrix";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";
    int memoryPerTask = 25;
    int memoryForMatch = memoryPerTask;
    int numIterations = 5;
    long numVertices = 4;

    if (args.length == 10) {
      degreeOfParallelism = Integer.parseInt(args[0]);
      numSubTasksPerInstance = Integer.parseInt(args[1]);
      pageWithRankInputPath = args[2];
      transitionMatrixInputPath = args[3];
      outputPath = args[4];
      confPath = args[5];
      memoryPerTask = Integer.parseInt(args[6]);
      memoryForMatch = Integer.parseInt(args[7]);
      numIterations = Integer.parseInt(args[8]);
      numVertices = Long.parseLong(args[9]);
    }

    JobGraph jobGraph = new JobGraph("PageRank");

    JobInputVertex pageWithRankInput = JobGraphUtils.createInput(PageWithRankInputFormat.class, pageWithRankInputPath,
        "PageWithRankInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig pageWithRankInputConfig = new TaskConfig(pageWithRankInput.getConfiguration());
    pageWithRankInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(pageWithRankInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobInputVertex transitionMatrixInput = JobGraphUtils.createInput(TransitionMatrixInputFormat.class,
        transitionMatrixInputPath, "TransitionMatrixInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig transitionMatrixInputConfig = new TaskConfig(transitionMatrixInput.getConfiguration());
    transitionMatrixInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(transitionMatrixInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "IterationIntermediate", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    intermediateConfig.setStubClass(DotProductMatch.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    intermediateConfig.setMemorySize(memoryForMatch * JobGraphUtils.MEGABYTE);
    intermediateConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.COMBININGSORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(DotProductReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0), new int[] { 0 },
        new Class[] { PactLong.class }, new boolean[] { true });
    tailConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(10);
    tailConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));

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

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connect(pageWithRankInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    JobGraphUtils.connect(head, intermediate, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    JobGraphUtils.connect(transitionMatrixInput, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    JobGraphUtils.connect(intermediate, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    fakeTailOutput.setVertexToShareInstancesWith(tail);
    tail.setVertexToShareInstancesWith(head);
    pageWithRankInput.setVertexToShareInstancesWith(head);
    transitionMatrixInput.setVertexToShareInstancesWith(head);
    intermediate.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
