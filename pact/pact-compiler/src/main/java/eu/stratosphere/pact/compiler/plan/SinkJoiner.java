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

import java.util.List;

import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;


/**
 * This class represents a utility node that is not part of the actual plan. It is used for plans with multiple data sinks to
 * transform it into a plan with a single root node. That way, the code that makes sure no costs are double-counted and that 
 * candidate selection works correctly with nodes that have multiple outputs is transparently reused.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SinkJoiner extends TwoInputNode
{	
	
	public SinkJoiner(OptimizerNode input1, OptimizerNode input2)
	{
		super(new NoContract());
		setLocalStrategy(LocalStrategy.NONE);
		
		PactConnection conn1 = new PactConnection(input1, this);
		PactConnection conn2 = new PactConnection(input2, this);
		
		conn1.setShipStrategy(ShipStrategy.FORWARD);
		conn2.setShipStrategy(ShipStrategy.FORWARD);
		
		setFirstInputConnection(conn1);
		setSecondInputConnection(conn2);
	}
	
	private SinkJoiner(SinkJoiner template, List<OptimizerNode> input1, List<OptimizerNode> input2) {
		super(template, input1, input2, template.getFirstInputConnection(), template.getSecondInputConnection(),
			template.getGlobalProperties(), template.getLocalProperties());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Internal Utility Node";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		// nothing to be done here
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// nothing to be done here
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getBranchesForParent(eu.stratosphere.pact.compiler.plan.OptimizerNode)
	 */
	@Override
	protected List<UnclosedBranchDescriptor> getBranchesForParent(OptimizerNode parent)
	{
		// return our own stack of open branches, because nothing is added
		return this.openBranches;
	}

	@Override
	protected void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations1,
			List<List<OptimizerNode>> alternativeSubPlanCominations2, CostEstimator estimator, List<OptimizerNode> outputPlans)
	{

		for(List<OptimizerNode> predList1 : alternativeSubPlanCominations1) {
			for(List<OptimizerNode> predList2 : alternativeSubPlanCominations2) {
				// check, whether the two children have the same
				// sub-plan in the common part before the branches
				if (!areBranchCompatible(predList1, predList2)) {
					continue;
				}
				
				SinkJoiner n = new SinkJoiner(this, predList1, predList2);
				estimator.costOperator(n);
				
				outputPlans.add(n);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
	}
	
	public void getDataSinks(List<DataSinkNode> target)
	{
		for(PactConnection c : this.input1) {
			OptimizerNode input1 = c.getSourcePact();

			if (input1 instanceof DataSinkNode) {
				target.add((DataSinkNode) input1); 
			}
			else {
				((SinkJoiner) input1).getDataSinks(target);
			}
			
		}
		
		for(PactConnection c : this.input2) {
			OptimizerNode input2 = c.getSourcePact();
		
			if (input2 instanceof DataSinkNode) {
				target.add((DataSinkNode) input2); 
			}
			else {
				((SinkJoiner) input2).getDataSinks(target);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Mock classes that represents a contract without behavior.
	// ------------------------------------------------------------------------
	
	private static final class MockStub extends Stub {}
	
	private static final class NoContract extends DualInputContract<MockStub>
	{
		private NoContract() {
			super(MockStub.class, "NoContract");
		}
	}

}
