/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.generic.stub.GenericMatcher;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithAdjacencyList;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRank;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRankAndDangling;

import java.util.Random;
import java.util.Set;

public class CustomCompensatableDotProductMatch extends AbstractStub implements
		GenericMatcher<VertexWithRankAndDangling, VertexWithAdjacencyList, VertexWithRank> {

	private VertexWithRank record = new VertexWithRank();

	private Random random = new Random();
	
	private double messageLoss;
	
	private boolean isFailure;

	@Override
	public void open(Configuration parameters) throws Exception {
		int workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
		int currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
		int failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		Set<Integer> failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		isFailure = currentIteration == failingIteration && failingWorkers.contains(workerIndex);
		messageLoss = ConfigUtils.asDouble("compensation.messageLoss", parameters);
	}

	@Override
	public void match(VertexWithRankAndDangling pageWithRank, VertexWithAdjacencyList adjacencyList, Collector<VertexWithRank> collector)
	throws Exception
	{
		double rank = pageWithRank.getRank();
		long[] adjacentNeighbors = adjacencyList.getTargets();
		int numNeighbors = adjacencyList.getNumTargets();

		double rankToDistribute = rank / (double) numNeighbors;
		record.setRank(rankToDistribute);

		for (int n = 0; n < numNeighbors; n++) {
			record.setVertexID(adjacentNeighbors[n]);
			if (isFailure) {
				if (random.nextDouble() >= messageLoss) {
					collector.collect(record);
				}
			} else {
				collector.collect(record);
			}
		}
	}
}
