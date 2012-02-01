package eu.stratosphere.pact.programs.preparation;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
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
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.Longify;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.RankReduce;
import eu.stratosphere.pact.programs.inputs.DBPediaPageLinkInput;
import eu.stratosphere.pact.programs.preparation.tasks.AdjListOutput;
import eu.stratosphere.pact.programs.preparation.tasks.CreateAdjList;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PrepareDBPedia {	
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
		
		JobTaskVertex adjList = createTask(CreateAdjList.class, graph, dop, spi);
		adjList.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(adjList, baseMemory/3);
		setReduceInformation(adjList, RankReduce.class, 
				new int[] {0}, new Class[] {keyType});
		
		JobOutputVertex sinkVertex = createOutput(AdjListOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, longify, null, null);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, longify, adjList, 
				new int[] {0}, new Class[] {keyType});
		
		connectJobVertices(ShipStrategy.FORWARD, adjList, sinkVertex, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
