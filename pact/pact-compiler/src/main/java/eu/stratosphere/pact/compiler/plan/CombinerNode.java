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

package eu.stratosphere.pact.compiler.plan;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * TODO: add Java doc
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class CombinerNode extends OptimizerNode {
	private PactConnection input;

	public CombinerNode(ReduceContract reducer, OptimizerNode predecessor, float reducingFactor) {
		super(reducer);

		this.input = new PactConnection(predecessor, this, ShipStrategy.FORWARD);
		this.setLocalStrategy(LocalStrategy.COMBININGSORT);

		this.globalProps = predecessor.globalProps;
		this.localProps = predecessor.localProps;

		this.setDegreeOfParallelism(predecessor.getDegreeOfParallelism());
		this.setInstancesPerMachine(predecessor.getInstancesPerMachine());

		// set the estimates
		this.estimatedCardinality.putAll(predecessor.estimatedCardinality);
		
		long estKeyCard = getEstimatedCardinality(new FieldSet(getPactContract().getKeyColumnNumbers(0)));

		if (predecessor.estimatedNumRecords >= 1 && estKeyCard >= 1
			&& predecessor.estimatedOutputSize >= -1) {
			this.estimatedNumRecords = (long) (predecessor.estimatedNumRecords * reducingFactor);
			this.estimatedOutputSize = (long) (predecessor.estimatedOutputSize * reducingFactor);
		} else {
			this.estimatedNumRecords = predecessor.estimatedNumRecords;
			this.estimatedOutputSize = predecessor.estimatedOutputSize;
		}
		
		// copy the child's branch-plan map
		if (this.branchPlan == null) {
			this.branchPlan = predecessor.branchPlan;
		} else if (predecessor.branchPlan != null) {
			this.branchPlan.putAll(predecessor.branchPlan);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Combine";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case COMBININGSORT: return 1;
			default:	        return 0;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getPactContract()
	 */
	@Override
	public ReduceContract getPactContract() {
		return (ReduceContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public List<? extends OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input.getSourcePact().accept(visitor);
			visitor.postVisit(this);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldKept(int, int)
	 */
	@Override
	public boolean isFieldKept(int input, int fieldNumber) {
		return false;
	}


	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		// DO NOTHING
	}
	

}
