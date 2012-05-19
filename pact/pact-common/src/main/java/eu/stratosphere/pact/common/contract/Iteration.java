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
