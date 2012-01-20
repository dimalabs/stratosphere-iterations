package eu.stratosphere.pact.programs.bulkpagerank;

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
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.iterative.nephele.tasks.NonCachingIterativeMatch;
import eu.stratosphere.pact.iterative.nephele.tasks.ProbeCachingMatch;
import eu.stratosphere.pact.iterative.nephele.tasks.SortingReduce;
import eu.stratosphere.pact.programs.bulkpagerank.tasks.BulkGroupTask;
import eu.stratosphere.pact.programs.bulkpagerank.tasks.ContributionMatch;
import eu.stratosphere.pact.programs.bulkpagerank.tasks.DiffMatch;
import eu.stratosphere.pact.programs.bulkpagerank.tasks.InitialRankAssigner;
import eu.stratosphere.pact.programs.bulkpagerank.tasks.RankReduce;
import eu.stratosphere.pact.programs.pagerank.tasks.DBPediaPageLinkInput;
import eu.stratosphere.pact.programs.pagerank.tasks.RankOutput;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class BulkPageRank {	
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
		
		JobGraph graph = new JobGraph("Bulk PageRank Broadcast");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(DBPediaPageLinkInput.class, input, graph, dop, spi);
		
		JobTaskVertex adjList = createTask(BulkGroupTask.class, graph, dop, spi);
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
		
		//Inner iteration loop tasks -- START
		JobTaskVertex contributionMatch = createTask(ProbeCachingMatch.class, graph, dop, spi);
		contributionMatch.setVertexToShareInstancesWith(sourceVertex);
		setMatchInformation(contributionMatch, ContributionMatch.class, 
				new int[] {0}, new int[] {0}, new Class[] {PactString.class});
		setMemorySize(contributionMatch, baseMemory);
		
		JobTaskVertex rankReduce = createTask(SortingReduce.class, graph, dop, spi);
		rankReduce.setVertexToShareInstancesWith(sourceVertex);
		setReduceInformation(rankReduce, RankReduce.class, 
				new int[] {0}, new Class[] {PactString.class});
		setMemorySize(rankReduce, baseMemory/3);
		
		JobTaskVertex diffMatch = createTask(NonCachingIterativeMatch.class, graph, dop, spi);
		diffMatch.setVertexToShareInstancesWith(sourceVertex);
		setMatchInformation(diffMatch, DiffMatch.class, 
				new int[] {0}, new int[] {0}, new Class[] {PactString.class});
		setMemorySize(diffMatch, baseMemory);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, adjList, 
				new int[] {0}, new Class[] {PactString.class});
		
		connectJobVertices(ShipStrategy.FORWARD, adjList, initialRankAssigner, null, null);
		connectJobVertices(ShipStrategy.FORWARD, initialRankAssigner, tmpTask, null, null);
		connectJobVertices(ShipStrategy.FORWARD, contributionMatch, rankReduce, null, null);
		
		connectBulkIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {contributionMatch, diffMatch}, 
				rankReduce,	diffMatch, ShipStrategy.BROADCAST, BulkPageRankTerminator.class, graph);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, adjList, contributionMatch, 
				new int[] {1}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, rankReduce, diffMatch, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
