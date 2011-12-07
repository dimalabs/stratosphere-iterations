package eu.stratosphere.pact.programs;

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
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.io.EdgeOutput;
import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationStateSynchronizer;
import eu.stratosphere.pact.iterative.nephele.tasks.core.IterationTail;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterationIterator;
import eu.stratosphere.pact.programs.pagerank.DBPediaPageLinkInput;
import eu.stratosphere.pact.programs.pagerank.GroupTask;
import eu.stratosphere.pact.programs.pagerank.PageRankIteration;
import eu.stratosphere.pact.programs.pagerank.PairOutput;
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
		
		JobOutputVertex sinkVertex = createOutput(PairOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.PARTITION_HASH, sourceVertex, adjList, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, adjList, iterationStart, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, iterationStart, iterationEnd, 
				new int[] {0}, new Class[] {PactString.class});
		connectJobVertices(ShipStrategy.FORWARD, iterationEnd, sinkVertex, null, null);
		
		//Iteration specific (make sure that iterationStart and iterationEnd share the same 
		//instance and subtask id structure. The synchronizer is required, so that a new
		//iteration does not start before all other subtasks are finished.
		JobTaskVertex iterationStateSynchronizer = createTask(IterationStateSynchronizer.class, graph, dop);
		iterationStateSynchronizer.setVertexToShareInstancesWith(sourceVertex);
		iterationStateSynchronizer.setNumberOfSubtasks(1);
		connectJobVertices(ShipStrategy.FORWARD, iterationStart, iterationEnd, null, null);
		connectJobVertices(ShipStrategy.BROADCAST, iterationEnd, iterationStateSynchronizer, null, null);
		connectJobVertices(ShipStrategy.BROADCAST, iterationStart, iterationStateSynchronizer, null, null);
		JobOutputVertex dummySink = createOutput(EdgeOutput.class, output, graph, dop);
		dummySink.setVertexToShareInstancesWith(iterationStateSynchronizer);
		dummySink.setNumberOfSubtasks(1);
		connectJobVertices(ShipStrategy.FORWARD, iterationStateSynchronizer, dummySink, null, null);
		
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
