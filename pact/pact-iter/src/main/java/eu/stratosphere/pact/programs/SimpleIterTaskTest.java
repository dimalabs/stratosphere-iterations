package eu.stratosphere.pact.programs;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectIterationLoop;
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
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.io.EdgeInput;
import eu.stratosphere.pact.iterative.nephele.io.EdgeOutput;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationTail;
import eu.stratosphere.pact.iterative.nephele.util.TenRoundTermination;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class SimpleIterTaskTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		if(args.length != 3) {
			System.exit(-1);
		}
		
		final int dop = Integer.parseInt(args[0]);
		final String input = args[1];
		final String output = args[2];
		
		JobGraph graph = new JobGraph("Iterative Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);
		
		JobTaskVertex iterationStart = createTask(DummyIterationHead.class, graph, dop);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationEnd = createTask(IterationTail.class, graph, dop);
		iterationEnd.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, iterationStart, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStart, iterationEnd, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, sinkVertex, null, null);
		
		connectIterationLoop(iterationStart, iterationEnd, TenRoundTermination.class, graph);
		
		//Submit job
		submit(graph, getConfiguration());
	}
	
	public static class DummyIterationHead extends IterationHead {

		@Override
		public void processInput(MutableObjectIterator<PactRecord> iter) throws Exception {
			PactRecord rec = new PactRecord();
			while(iter.next(rec)) {
			}
			
			//Inject two dummy records in the iteration process
			for (int i = 0; i < 100000; i++) {
				rec.setField(0, new PactInteger(i));
				output.getWriters().get(0).emit(rec);
			}
		}

		@Override
		public void processUpdates(MutableObjectIterator<PactRecord> iter) throws Exception {
			PactRecord rec = new PactRecord();
			while(iter.next(rec)) {
				output.getWriters().get(0).emit(rec);
			}
		}

		@Override
		public void finish() throws Exception {
			PactRecord rec = new PactRecord();
			rec.setField(0, new PactInteger(0));
			rec.setField(1, new PactInteger(1));
			output.getWriters().get(1).emit(rec);
		}
		
	}
}
