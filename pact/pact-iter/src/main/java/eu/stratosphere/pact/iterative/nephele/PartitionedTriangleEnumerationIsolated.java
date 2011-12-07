package eu.stratosphere.pact.iterative.nephele;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setProperty;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

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
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.AdjacencyListOrdering;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.CacheBuild;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.CountTriangles;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.DuplicateEdgesHashPartitioning;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.SendDegreesHashPartitioned;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.SendTrianglesHashPartitioned;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.SetDegrees;
import eu.stratosphere.pact.iterative.nephele.tasks.triangle.SetTriangles;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PartitionedTriangleEnumerationIsolated {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		final int dop = Integer.parseInt(args[0]);
		final String input = args[1];
		final String output = args[2];
		
		JobGraph graph = new JobGraph("Iterative Test");
		if(args.length > 3) {
			graph.addJar(new Path(args[3]));
		}
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);
		
		JobTaskVertex emitDirectedEdges = createTask(DuplicateEdgesHashPartitioning.class, graph, dop);
		emitDirectedEdges.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex buildCache = createTask(CacheBuild.class, graph, dop);
		buildCache.setVertexToShareInstancesWith(sourceVertex);
		setProperty(buildCache, CacheBuild.CACHE_ID_PARAM, "edges");
		setProperty(buildCache, CacheBuild.CACHE_TYPE_PARAM, CacheType.ISOLATED.name());
		
		JobTaskVertex orderCache = createTask(AdjacencyListOrdering.class, graph, dop);
		orderCache.setVertexToShareInstancesWith(sourceVertex);
		setProperty(orderCache, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobTaskVertex sendDegree = createTask(SendDegreesHashPartitioned.class, graph, dop);
		sendDegree.setVertexToShareInstancesWith(sourceVertex);
		setProperty(sendDegree, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobTaskVertex setDegree = createTask(SetDegrees.class, graph, dop);
		setDegree.setVertexToShareInstancesWith(sourceVertex);
		setProperty(setDegree, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobTaskVertex sendTriangles = createTask(SendTrianglesHashPartitioned.class, graph, dop);
		sendTriangles.setVertexToShareInstancesWith(sourceVertex);
		setProperty(sendTriangles, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobTaskVertex setTriangles = createTask(SetTriangles.class, graph, dop);
		setTriangles.setVertexToShareInstancesWith(sourceVertex);
		setProperty(setTriangles, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobTaskVertex countTriangles = createTask(CountTriangles.class, graph, dop);
		countTriangles.setVertexToShareInstancesWith(sourceVertex);
		setProperty(countTriangles, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, emitDirectedEdges, null, null);
		connectJobVertices(ShipStrategy.PARTITION_RANGE, emitDirectedEdges, buildCache, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.BROADCAST, buildCache, orderCache, null, null);
		connectJobVertices(ShipStrategy.BROADCAST, orderCache, sendDegree, null, null);
		connectJobVertices(ShipStrategy.PARTITION_RANGE, sendDegree, setDegree, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.BROADCAST, setDegree, sendTriangles, null, null);
		connectJobVertices(ShipStrategy.PARTITION_RANGE, sendTriangles, setTriangles, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.BROADCAST, setTriangles, countTriangles, null, null);
		connectJobVertices(ShipStrategy.FORWARD, countTriangles, sinkVertex, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
