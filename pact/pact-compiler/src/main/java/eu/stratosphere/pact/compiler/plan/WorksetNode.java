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

import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetPlanNode;
import eu.stratosphere.pact.generic.contract.WorksetIteration.WorksetPlaceHolder;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk iteration.
 */
public class WorksetNode extends AbstractPartialSolutionNode
{
	private final WorksetIterationNode iterationNode;
	
	
	public WorksetNode(WorksetPlaceHolder psph, WorksetIterationNode iterationNode) {
		super(psph);
		this.iterationNode = iterationNode;
	}

	// --------------------------------------------------------------------------------------------
	
	public void setCandidateProperties(GlobalProperties gProps, LocalProperties lProps) {
		if (this.cachedPlans != null) {
			throw new IllegalStateException();
		} else {
			this.cachedPlans = Collections.<PlanNode>singletonList(new WorksetPlanNode(this, gProps, lProps));
		}
	}
	
	public WorksetPlanNode getCurrentWorksetPlanNode() {
		if (this.cachedPlans != null) {
			return (WorksetPlanNode) this.cachedPlans.get(0);
		} else {
			throw new IllegalStateException();
		}
	}
	
	public WorksetIterationNode getIterationNode() {
		return this.iterationNode;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public WorksetPlaceHolder getPactContract() {
		return (WorksetPlaceHolder) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Workset";
	}
}
