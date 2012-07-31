package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.sopremo.serialization.NaiveSchemaFactory;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.serialization.SchemaFactory;

/**
 * Encapsulate a complete query in Sopremo and translates it to a Pact {@link Plan}.
 * 
 * @author Arvid Heise
 */
public class SopremoPlan {
	private final SopremoModule module;

	private EvaluationContext context = new EvaluationContext();

	private SchemaFactory schemaFactory = new NaiveSchemaFactory();

	public SopremoPlan() {
		this.module = new SopremoModule("plan", 0, 0);
		this.context.getFunctionRegistry().register(DefaultFunctions.class);
	}

	/**
	 * Converts the Sopremo module to a Pact {@link Plan}.
	 * 
	 * @return the converted Pact plan
	 */
	public Plan asPactPlan() {
		return new Plan(this.checkForSinks(this.assemblePact()));
	}

	/**
	 * Checks if all contracts are {@link GenericDataSink}s.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Collection<GenericDataSink> checkForSinks(Collection<Contract> contracts) {
		for (Contract contract : contracts)
			if (!GenericDataSink.class.isInstance(contract))
				throw new IllegalStateException("Contract without connected sink detected " + contract);
		return (Collection) contracts;
	}

	public void setSinks(final Sink... sinks) {
		this.setSinks(Arrays.asList(sinks));
	}

	public void setSinks(final List<Sink> sinks) {
		for (final Sink sink : sinks)
			this.module.addInternalOutput(sink);
	}

	public Schema getSchema() {
		return this.context.getOutputSchema(0);
	}

	/**
	 * Assembles the Pacts of the contained Sopremo operators and returns a list
	 * of all Pact sinks. These sinks may either be directly a {@link FileDataSinkContract} or an unconnected
	 * {@link Contract}.
	 * 
	 * @return a list of Pact sinks
	 */
	public Collection<Contract> assemblePact() {
		final ElementarySopremoModule elementaryModule = this.module.asElementary();
		elementaryModule.inferSchema(this.schemaFactory);
		this.context.setSchema(elementaryModule.getSchema());
		return elementaryModule.assemblePact(this.context);
	}

	/**
	 * Returns all operators that are either (internal) {@link Sink}s or
	 * included in the reference graph.
	 * 
	 * @return all operators in this module
	 */
	public Iterable<? extends Operator<?>> getContainedOperators() {
		return this.module.getReachableNodes();
	}

	/**
	 * Returns the evaluation context of this plan.
	 * 
	 * @return the evaluation context
	 */
	public EvaluationContext getContext() {
		return this.context;
	}

	/**
	 * Sets the evaluation context of this plan.
	 * 
	 * @param context
	 *        the evaluation context
	 */
	public void setContext(final EvaluationContext context) {
		if (context == null)
			throw new NullPointerException("context must not be null");

		this.context = context;
	}

	@Override
	public String toString() {
		return this.module.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.module.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final SopremoPlan other = (SopremoPlan) obj;
		return this.module.equals(other.module);
	}

	public List<Operator<?>> getUnmatchingOperators(final SopremoPlan other) {
		return this.module.getUnmatchingNodes(other.module);
	}
}
