package eu.stratosphere.pact.test;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore.CacheType;
import eu.stratosphere.pact.iterative.nephele.io.EdgeInput;
import eu.stratosphere.pact.iterative.nephele.io.EdgeOutput;
import eu.stratosphere.pact.iterative.nephele.tasks.AdjacencyListOrdering;
import eu.stratosphere.pact.iterative.nephele.tasks.CacheBuild;
import eu.stratosphere.pact.iterative.nephele.tasks.CountTriangles;
import eu.stratosphere.pact.iterative.nephele.tasks.DuplicateEdges;
import eu.stratosphere.pact.iterative.nephele.tasks.SendDegrees;
import eu.stratosphere.pact.iterative.nephele.tasks.SendTriangles;
import eu.stratosphere.pact.iterative.nephele.tasks.SetDegrees;
import eu.stratosphere.pact.iterative.nephele.tasks.SetTriangles;
import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractIterativeMinimalTask;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationStart;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.*;

public class IterTaskTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		final int dop = 2;
		final String input = "file:///home/mkaufmann/dummy";
		final String output = "file:///home/mkaufmann/iter-test";
		
		JobGraph graph = new JobGraph("Iterative Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);
		
		JobTaskVertex iterationStart = createTask(IterationStart.class, graph, dop);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationStep = createTask(DummyIterationStep.class, graph, dop);
		iterationStep.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, iterationStart, null, null);
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationStep, null, null);
		connectJobVertices(ShipStrategy.FORWARD, iterationStep, sinkVertex, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
	
	public static class DummyIterationStep extends AbstractIterativeMinimalTask {
		@Override
		protected void initTask() {
			
		}

		@Override
		public int getNumberOfInputs() {
			return 1;
		}
	}
}
