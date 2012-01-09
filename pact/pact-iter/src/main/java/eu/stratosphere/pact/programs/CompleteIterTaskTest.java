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
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationTail;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class CompleteIterTaskTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		final int dop = 2;
		final String input = "file:///home/mkaufmann/dummy";
		final String output = "file:///home/mkaufmann/iter-test";
		
		JobGraph graph = new JobGraph("Iterative Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);
		
		JobTaskVertex iterationStart = createTask(DummyIterationHead.class, graph, dop);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		iterationStart.getConfiguration().setLong(IterationHead.MEMORY_SIZE, 32);
		
		JobTaskVertex iterationStep = createTask(DummyIterationStep.class, graph, dop);
		iterationStep.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationEnd = createTask(IterationTail.class, graph, dop);
		iterationEnd.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, iterationStart, null, null);
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationStep, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStep, iterationEnd, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, sinkVertex, null, null);
		
		connectIterationLoop(iterationStart, iterationEnd, graph);
		
		//Submit job
		submit(graph, getConfiguration());
	}
	
	public static class DummyIterationStep extends AbstractIterativeTask {
		@Override
		protected void initTask() {
			
		}

		@Override
		public int getNumberOfInputs() {
			return 1;
		}

		@Override
		public void invokeIter(IterationIterator iter)
				throws Exception {
			PactRecord rec = new PactRecord();
			while(iter.next(rec)) {
				output.collect(rec);
			}
		}
	}
	
	public static class DummyIterationHead extends IterationHead {

		@Override
		public void processInput(MutableObjectIterator<PactRecord> iter) throws Exception {
			PactRecord rec = new PactRecord();
			while(iter.next(rec)) {
			}
			
			//Inject two dummy records in the iteration process
			for (int i = 0; i < 50; i++) {
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
		}
		
	}
}
