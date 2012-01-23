package eu.stratosphere.pact.programs.pagerank;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectBoundedRoundsIterationLoop;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationTail;
import eu.stratosphere.pact.programs.pagerank.tasks.DBPediaPageLinkInput;
import eu.stratosphere.pact.programs.pagerank.tasks.GroupTask;
import eu.stratosphere.pact.programs.pagerank.tasks.HashingPageRankIteration;
import eu.stratosphere.pact.programs.pagerank.tasks.RankOutput;
import eu.stratosphere.pact.programs.pagerank.tasks.SortingPageRankIteration;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PageRank {
	
	private enum PageRankVariants {
		HASH(HashingPageRankIteration.class), 
		SORT(SortingPageRankIteration.class);
		
		private Class<? extends AbstractMinimalTask> className;
		
		private PageRankVariants(Class<? extends AbstractMinimalTask> className) {
			this.className = className;
		}
		
		public Class<? extends AbstractMinimalTask> getPageRankClass() {
			return className;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		if(args.length != 6) {
			System.exit(-1);
		}
		
		final int dop = Integer.valueOf(args[0]);
		final String input = args[1];
		final String output = args[2];
		final Class<? extends AbstractMinimalTask> pageRankClass = 
				PageRankVariants.valueOf(args[3]).getPageRankClass(); 
		final int spi = Integer.valueOf(args[4]);
		
		JobGraph graph = new JobGraph("PageRank Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(DBPediaPageLinkInput.class, input, graph, dop, spi);
		
		JobTaskVertex adjList = createTask(GroupTask.class, graph, dop, spi);
		adjList.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationStart = createTask(pageRankClass, graph, dop, spi);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationEnd = createTask(IterationTail.class, graph, dop, spi);
		iterationEnd.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, adjList, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, adjList, iterationStart, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStart, iterationEnd, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, sinkVertex, null, null);
		
		connectBoundedRoundsIterationLoop(adjList, sinkVertex, null, null, iterationStart, ShipStrategy.PARTITION_HASH, 
				13, graph);
		//connectIterationLoop(iterationStart, iterationEnd, PageRankTerminator.class, graph);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
