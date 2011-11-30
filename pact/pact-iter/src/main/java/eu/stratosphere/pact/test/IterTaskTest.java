package eu.stratosphere.pact.test;

import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.NepheleUtil.submit;

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
import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationTail;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterationIterator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

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
		
		JobTaskVertex iterationStart = createTask(DummyIterationHead.class, graph, dop);
		iterationStart.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationStep = createTask(DummyIterationStep.class, graph, dop);
		iterationStep.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex iterationEnd = createTask(IterationTail.class, graph, dop);
		iterationStep.setVertexToShareInstancesWith(sourceVertex);
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, iterationStart, null, null);
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationStep, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStep, iterationEnd, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationEnd, sinkVertex, null, null);
		
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationEnd, null, null);
		
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
		
	}
}
