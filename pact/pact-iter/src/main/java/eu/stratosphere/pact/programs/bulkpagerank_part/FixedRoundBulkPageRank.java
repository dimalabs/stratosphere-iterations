package eu.stratosphere.pact.programs.bulkpagerank_part;

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
import eu.stratosphere.pact.iterative.nephele.tasks.BuildCachingMatch;
import eu.stratosphere.pact.iterative.nephele.tasks.SortingReduce;
import eu.stratosphere.pact.programs.bulkpagerank_opt.tasks.GroupNeighbours;
import eu.stratosphere.pact.programs.bulkpagerank_opt.tasks.InitialRankAssigner;
import eu.stratosphere.pact.programs.bulkpagerank_opt.tasks.Longify;
import eu.stratosphere.pact.programs.bulkpagerank_opt.tasks.RankOutput;
import eu.stratosphere.pact.programs.bulkpagerank_opt.tasks.RankReduce;
import eu.stratosphere.pact.programs.bulkpagerank_part.tasks.ContributionMatch;
import eu.stratosphere.pact.programs.inputs.DBPediaPageLinkInput;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class FixedRoundBulkPageRank {	
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
		JobInputVertex sourceVertex = createInput(DBPediaPageLinkInput.class, input, graph, dop, spi);
		
		JobTaskVertex longify = createTask(Longify.class, graph, dop, spi);
		longify.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex adjList = createTask(GroupNeighbours.class, graph, dop, spi);
		adjList.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(adjList, baseMemory/3);
		setReduceInformation(adjList, RankReduce.class, 
				new int[] {0}, new Class[] {keyType});
		
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
		
		//Inner iteration loop tasks -- START
		JobTaskVertex contributionMatch = createTask(BuildCachingMatch.class, graph, dop, spi);
		contributionMatch.setVertexToShareInstancesWith(sourceVertex);
		setMatchInformation(contributionMatch, ContributionMatch.class, 
				new int[] {0}, new int[] {0}, new Class[] {keyType});
		setMemorySize(contributionMatch, baseMemory);
		
		JobTaskVertex rankReduce = createTask(SortingReduce.class, graph, dop, spi);
		rankReduce.setVertexToShareInstancesWith(sourceVertex);
		setReduceInformation(rankReduce, RankReduce.class, 
				new int[] {0}, new Class[] {keyType});
		setMemorySize(rankReduce, baseMemory/3);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, longify, null, null);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, longify, adjList, 
				new int[] {0}, new Class[] {keyType});
		
		connectJobVertices(ShipStrategy.FORWARD, adjList, initialRankAssigner, null, null);
		connectJobVertices(ShipStrategy.FORWARD, initialRankAssigner, tmpTask, null, null);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, contributionMatch, rankReduce, 
				new int[] {0}, new Class[] {keyType});
		
		
		connectBulkIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {contributionMatch}, 
				rankReduce, 13, ShipStrategy.FORWARD, graph);
		
		connectJobVertices(ShipStrategy.FORWARD, adjList, contributionMatch, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
