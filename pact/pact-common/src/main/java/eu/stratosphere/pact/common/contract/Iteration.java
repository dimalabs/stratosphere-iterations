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

package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.plan.Visitor;

/**
 * @author Stephan Ewen
 */
public class Iteration extends Contract
{
	private class PlaceholderContract extends Contract {

		protected PlaceholderContract() {
			super("Placeholder");
		}

		@Override
		public void accept(Visitor<Contract> visitor) {
		}

		@Override
		public Class<?> getUserCodeClass() {
			return null;
		}
	}

	private final Contract partialSolutionPlaceholder = new PlaceholderContract();

	private Contract initialPartialSolution = null;

	private Contract nextPartialSolution = null;

	private Contract terminationCriterion = null;

	private int numIterations = -1;

	public Iteration() {
		super("Iteration");
	}

	/**
	 * @param name
	 */
	public Iteration(String name) {
		super(name);
	}

	public Contract getPartialSolution()
	{
		return this.partialSolutionPlaceholder;
	}

	public Contract getInitialPartialSolution()
	{
		return initialPartialSolution;
	}

	public void setInitialPartialSolution(Contract input)
	{
		this.initialPartialSolution = input;
	}

	public Contract getNextPartialSolution() {
		return this.nextPartialSolution;
	}

	public void setNextPartialSolution(Contract result)
	{
		this.nextPartialSolution = result;
	}

	public Contract getTerminationCriterion()
	{
		return terminationCriterion;
	}

	public void setTerminationCriterion(Contract criterion)
	{
		this.terminationCriterion = criterion;
	}

	public int getNumberOfIterations() {
		return this.numIterations;
	}

	public void setNumberOfIteration(int num)
	{
		this.numIterations = Math.max(num, -1);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<?> getUserCodeClass()
	{
		return null;
	}
}
