/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.iterative;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

/**
 *
 */
public class WorksetConnectedComponents implements PlanAssembler, PlanAssemblerDescription {
	
	public class NeighborWithComponentIDJoin extends MatchStub {

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out) {
			this.result.setField(0, edge.getField(1, PactLong.class));
			this.result.setField(1, vertexWithComponent.getField(1, PactLong.class));
			out.collect(this.result);
		}
	}
	
	@Combinable
	public class MinimumComponentIDReduce extends ReduceStub {

		private final PactRecord result = new PactRecord();
		private final PactLong vertexId = new PactLong();
		private final PactLong minComponentId = new PactLong();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {

			final PactRecord first = records.next();
			final long vertexID = first.getField(0, PactLong.class).getValue();
			
			long minimumComponentID = first.getField(1, PactLong.class).getValue();

			while (records.hasNext()) {
				long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			this.vertexId.setValue(vertexID);
			this.minComponentId.setValue(minimumComponentID);
			this.result.setField(0, this.vertexId);
			this.result.setField(1, this.minComponentId);
			out.collect(this.result);
		}
	}
	
	public class UpdateComponentIdMatch extends MatchStub {

		@Override
		public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent, Collector<PactRecord> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for the vertices
		FileDataSource initialVertices = new FileDataSource(DuplicateLongInputFormat.class, verticesInput, "Vertices");
		DelimitedInputFormat.configureDelimitedFormat(initialVertices).recordDelimiter('\n');
		
		WorksetIteration iteration = new WorksetIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(initialVertices);
		iteration.setInitialWorkset(initialVertices);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(LongLongInputFormat.class, edgeInput, "Edges");
		DelimitedInputFormat.configureDelimitedFormat(edges).recordDelimiter('\n');

		// create CrossContract for distance computation
		MatchContract joinWithNeighbors = MatchContract.builder(NeighborWithComponentIDJoin.class, PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract minCandidateId = ReduceContract.builder(MinimumComponentIDReduce.class, PactLong.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// create CrossContract for distance computation
		MatchContract updateComponentId = MatchContract.builder(UpdateComponentIdMatch.class, PactLong.class, 0, 0)
				.input1(minCandidateId)
				.input2(iteration.getSolutionSet())
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, iteration, "Result");

		// return the PACT plan
		Plan plan = new Plan(result, "Iterative KMeans");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.example.datamining.KMeansIteration#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <vertices> <edges> <out> <maxIterations>";
	}
}
