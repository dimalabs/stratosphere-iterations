package eu.stratosphere.pact.programs.bulkpagerank_broad;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectBulkIterationLoop;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setMatchInformation;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setMemorySize;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setReduceInformation;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.PreSortedReduce;
import eu.stratosphere.pact.iterative.nephele.tasks.ProbeCachingMatch;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.ContributionMatch;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.Forward;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.InitialRankAssigner;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.PrepareNeighbours;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.RankOutput;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.RankReduce;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.SortByNeighbour;
import eu.stratosphere.pact.programs.inputs.AdjListInput;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class GroupedFixedRoundBulkPageRank {	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		if(args.length != 5) {
			System.out.println("Not correct parameters");
			System.exit(-1);
		}
		
		final int dop = Integer.valueOf(args[0]);
		final String input = args[1];
		final String output = args[2];
		final int spi = Integer.valueOf(args[3]);
		final int baseMemory = Integer.valueOf(args[4]);
		final Class<? extends Key> keyType = PactLong.class;
		
		JobGraph graph = new JobGraph("Bulk PageRank Broadcast -- Optimized Twitter");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(AdjListInput.class, input, graph, dop, spi);
		
		JobTaskVertex adjList = createTask(PrepareNeighbours.class, graph, dop, spi);
		adjList.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex initialRankAssigner = createTask(InitialRankAssigner.class, graph, dop, spi);
		initialRankAssigner.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex tmpTask = new JobTaskVertex("TempTask", graph);
		tmpTask.setTaskClass(TempTask.class);
		tmpTask.setNumberOfSubtasks(dop);
		tmpTask.setNumberOfSubtasksPerInstance(spi);
		tmpTask.setVertexToShareInstancesWith(sourceVertex);
		TaskConfig tempConfig = new TaskConfig(tmpTask.getConfiguration());
		tempConfig.setStubClass(RankReduce.class);
		setMemorySize(tmpTask, baseMemory / 8);
		
		JobTaskVertex sortedNeighbours = createTask(SortByNeighbour.class, graph, dop, spi);
		sortedNeighbours.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(sortedNeighbours, baseMemory/3);
		setReduceInformation(sortedNeighbours, RankReduce.class, 
				new int[] {1}, new Class[] {keyType});
		
		//Inner iteration loop tasks -- START
		JobTaskVertex forward = createTask(Forward.class, graph, dop, spi);
		forward.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex contributionMatch = createTask(ProbeCachingMatch.class, graph, dop, spi);
		contributionMatch.setVertexToShareInstancesWith(sourceVertex);
		setMatchInformation(contributionMatch, ContributionMatch.class, 
				new int[] {0}, new int[] {0}, new Class[] {keyType});
		setMemorySize(contributionMatch, baseMemory);
		
		JobTaskVertex rankReduce = createTask(PreSortedReduce.class, graph, dop, spi);
		rankReduce.setVertexToShareInstancesWith(sourceVertex);
		setReduceInformation(rankReduce, RankReduce.class, 
				new int[] {0}, new Class[] {keyType});
		//setMemorySize(rankReduce, baseMemory/3);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks		
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, adjList, 
				new int[] {0}, new Class[] {keyType});
		
		connectJobVertices(ShipStrategy.FORWARD, adjList, initialRankAssigner, null, null);
		connectJobVertices(ShipStrategy.FORWARD, initialRankAssigner, tmpTask, null, null);
		
		connectJobVertices(ShipStrategy.BROADCAST, forward, contributionMatch, null, null);
		connectJobVertices(ShipStrategy.FORWARD, contributionMatch, rankReduce, null, null);
		
		connectBulkIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {forward}, 
				rankReduce, 13, ShipStrategy.FORWARD, graph);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, adjList, sortedNeighbours, 
				new int[] {1}, new Class[] {keyType});
		connectJobVertices(ShipStrategy.FORWARD, sortedNeighbours, contributionMatch, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
