package eu.stratosphere.pact.generic.contract;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;

/**
 * @author Stephan Ewen
 */
public class WorksetIteration extends Contract
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

	private final Contract worksetPlaceholder = new PlaceholderContract();

	/**
	 * The classes that represent the solution key data types.
	 */
	private final Class<? extends Key>[] keyClasses;

	/**
	 * The positions of the keys in the solution tuple.
	 */
	private final int[] keyFields;

	private Contract initialPartialSolution = null;

	private Contract initialWorkset = null;

	private Contract partialSolutionDelta = null;

	private Contract nextWorkset = null;

	public WorksetIteration(Class<? extends Key>[] keyTypes, int[] keyPositions) {
		super("WorksetIteration");
		this.keyClasses = keyTypes;
		this.keyFields = keyPositions;
	}

	/**
	 * @param name
	 */
	public WorksetIteration(Class<? extends Key>[] keyTypes, int[] keyPositions, String name) {
		super(name);
		this.keyClasses = keyTypes;
		this.keyFields = keyPositions;
	}

	public Class<? extends Key>[] getKeyClasses() {
		return this.keyClasses;
	}

	public int[] getKeyColumns(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	public Contract getPartialSolution()
	{
		return this.partialSolutionPlaceholder;
	}

	public Contract getWorkset()
	{
		return this.worksetPlaceholder;
	}

	public Contract getInitialPartialSolution() {
		return this.initialPartialSolution;
	}

	public void setInitialPartialSolution(Contract input)
	{
		this.initialPartialSolution = input;
	}

	public Contract getPartialSolutionDelta() {
		return this.partialSolutionDelta;
	}

	public void setPartialSolutionDelta(Contract result)
	{
		this.partialSolutionDelta = result;
	}

	public Contract getInitialWorkset() {
		return this.initialWorkset;
	}

	public void setInitialWorkset(Contract input)
	{
		this.initialWorkset = input;
	}

	public Contract getNextWorkset() {
		return this.nextWorkset;
	}

	public void setNextWorkset(Contract result)
	{
		this.nextWorkset = result;
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
