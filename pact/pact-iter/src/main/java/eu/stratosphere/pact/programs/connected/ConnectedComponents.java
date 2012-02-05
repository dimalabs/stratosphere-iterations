package eu.stratosphere.pact.programs.connected;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectFixedPointIterationLoop;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setMatchInformation;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setMemorySize;
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
import eu.stratosphere.pact.iterative.nephele.tasks.TempTaskV2;
import eu.stratosphere.pact.iterative.nephele.tasks.UpdateableMatching;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.RankOutput;
import eu.stratosphere.pact.programs.bulkpagerank_broad.tasks.RankReduce;
import eu.stratosphere.pact.programs.connected.tasks.CidMatch;
import eu.stratosphere.pact.programs.connected.tasks.CountUpdates;
import eu.stratosphere.pact.programs.connected.tasks.EmptyTerminationDecider;
import eu.stratosphere.pact.programs.connected.tasks.InitialStateComponents;
import eu.stratosphere.pact.programs.connected.tasks.InitialUpdates;
import eu.stratosphere.pact.programs.connected.tasks.SendUpdates;
import eu.stratosphere.pact.programs.inputs.AdjListInput;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class ConnectedComponents {	
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
		
		JobGraph graph = new JobGraph("Connected Components");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(AdjListInput.class, input, graph, dop, spi);
		
		JobTaskVertex initialState = createTask(InitialStateComponents.class, graph, dop, spi);
		initialState.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex initialUpdateAssigner = createTask(InitialUpdates.class, graph, dop, spi);
		initialUpdateAssigner.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex tmpTask = new JobTaskVertex("TempTask", graph);
		tmpTask.setTaskClass(TempTaskV2.class);
		tmpTask.setNumberOfSubtasks(dop);
		tmpTask.setNumberOfSubtasksPerInstance(spi);
		tmpTask.setVertexToShareInstancesWith(sourceVertex);
		TaskConfig tempConfig = new TaskConfig(tmpTask.getConfiguration());
		tempConfig.setStubClass(RankReduce.class);
		setMemorySize(tmpTask, baseMemory / 8);
		
		//Inner iteration loop tasks -- START		
		JobTaskVertex idMatch = createTask(UpdateableMatching.class, graph, dop, spi);
		idMatch.setVertexToShareInstancesWith(sourceVertex);
		setMatchInformation(idMatch, CidMatch.class, 
				new int[] {0}, new int[] {0},  new Class[] {keyType});
		setMemorySize(idMatch, baseMemory/3);
		
		JobTaskVertex distributeUpdates = createTask(SendUpdates.class, graph, dop, spi);
		distributeUpdates.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex countUpdates = createTask(CountUpdates.class, graph, dop, spi);
		countUpdates.setVertexToShareInstancesWith(sourceVertex);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, initialState, 
				new int[] {0}, new Class[] {keyType});
		
		connectJobVertices(ShipStrategy.FORWARD, initialState, initialUpdateAssigner, null, null);
		connectJobVertices(ShipStrategy.FORWARD, initialUpdateAssigner, tmpTask, null, null);
		
		
		connectFixedPointIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {distributeUpdates,
				countUpdates}, 
				distributeUpdates, countUpdates, idMatch, 
				ShipStrategy.PARTITION_HASH, 
				EmptyTerminationDecider.class, graph);
		
		connectJobVertices(ShipStrategy.FORWARD, initialState, idMatch, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
