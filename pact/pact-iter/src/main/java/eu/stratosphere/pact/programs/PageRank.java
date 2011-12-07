package eu.stratosphere.pact.programs;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createDummyOutput;
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
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationStateSynchronizer;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationTail;
import eu.stratosphere.pact.programs.pagerank.DBPediaPageLinkInput;
import eu.stratosphere.pact.programs.pagerank.GroupTask;
import eu.stratosphere.pact.programs.pagerank.PageRankIteration;
import eu.stratosphere.pact.programs.pagerank.RankOutput;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PageRank {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		final int dop = 2;
		//final String input = "file:///home/mkaufmann/data/page_links_el.nt";
		final String input = "file:///home/mkaufmann/data/xaa";
		final String output = "file:///home/mkaufmann/iter-test";
		
		JobGraph graph = new JobGraph("PageRank Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(DBPediaPageLinkInput.class, input, graph, dop);
		
		JobTaskVertex adjList = createTask(GroupTask.class, graph, dop);
		adjList.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationStart = createTask(PageRankIteration.class, graph, dop);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationEnd = createTask(IterationTail.class, graph, dop);
		iterationEnd.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, adjList, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, adjList, iterationStart, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStart, iterationEnd, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, sinkVertex, null, null);
		
		//Iteration specific (make sure that iterationStart and iterationEnd share the same 
		//instance and subtask id structure. The synchronizer is required, so that a new
		//iteration does not start before all other subtasks are finished.
		JobOutputVertex dummySinkA = createDummyOutput(graph, dop);
		dummySinkA.setVertexToShareInstancesWith(sourceVertex);
		connectJobVertices(ShipStrategy.FORWARD, iterationEnd, dummySinkA, null, null);
		JobTaskVertex iterationStateSynchronizer = createTask(IterationStateSynchronizer.class, graph, dop);
		iterationStateSynchronizer.setVertexToShareInstancesWith(sourceVertex);
		iterationStateSynchronizer.setNumberOfSubtasks(1);
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationEnd, null, null);
		connectJobVertices(ShipStrategy.BROADCAST, iterationEnd, iterationStateSynchronizer, null, null);
		connectJobVertices(ShipStrategy.BROADCAST, iterationStart, iterationStateSynchronizer, null, null);
		JobOutputVertex dummySinkB = createDummyOutput(graph, dop);
		dummySinkB.setVertexToShareInstancesWith(sourceVertex);
		connectJobVertices(ShipStrategy.FORWARD, iterationStateSynchronizer, dummySinkB, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
