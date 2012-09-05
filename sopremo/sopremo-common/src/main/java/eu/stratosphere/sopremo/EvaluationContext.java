package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.DefaultFunctionRegistry;
import eu.stratosphere.sopremo.packages.EvaluationScope;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.serialization.ObjectSchema;
import eu.stratosphere.sopremo.serialization.Schema;

/**
 * Provides additional context to the evaluation of {@link Evaluable}s, such as access to all registered functions.
 * 
 * @author Arvid Heise
 */
public class EvaluationContext extends AbstractSopremoType implements ISerializableSopremoType, EvaluationScope {
	private static final long serialVersionUID = 7701485388451926506L;

	private final IFunctionRegistry methodRegistry;

	private final IConstantRegistry constantRegistry;

	private int inputCounter = 0;

	private final LinkedList<Operator<?>> operatorStack = new LinkedList<Operator<?>>();

	private Schema[] inputSchemas, outputSchemas;

	private Schema schema;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	public LinkedList<Operator<?>> getOperatorStack() {
		return this.operatorStack;
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	public void setResultProjection(final EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	public String operatorTrace() {
		final Iterator<Operator<?>> descendingIterator = this.operatorStack.descendingIterator();
		final StringBuilder builder = new StringBuilder(descendingIterator.next().getName());
		while (descendingIterator.hasNext())
			builder.append("->").append(descendingIterator.next().getName());
		return builder.toString();
	}

	public Operator<?> getCurrentOperator() {
		return this.operatorStack.peek();
	}

	public void pushOperator(final Operator<?> e) {
		// reset inputs to avoid serialization
		this.operatorStack.push(e.clone().withInputs(new JsonStream[e.getMinInputs()]));
	}

	public Operator<?> popOperator() {
		return this.operatorStack.pop();
	}

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext(final int numInputs, final int numOutputs, IFunctionRegistry methodRegistry,
			IConstantRegistry constantRegistry) {
		this.methodRegistry = methodRegistry;
		this.constantRegistry = constantRegistry;
		this.setInputsAndOutputs(numInputs, numOutputs);
	}

	public void setInputsAndOutputs(final int numInputs, final int numOutputs) {
		this.inputSchemas = new Schema[numInputs];
		Arrays.fill(this.inputSchemas, new ObjectSchema());
		this.outputSchemas = new Schema[numOutputs];
		Arrays.fill(this.outputSchemas, new ObjectSchema());
	}

	public EvaluationContext(final EvaluationContext context) {
		this(context.inputSchemas.length, context.outputSchemas.length, context.methodRegistry,
			context.constantRegistry);
		this.inputCounter = context.inputCounter;
		this.inputSchemas = context.inputSchemas.clone();
		this.outputSchemas = context.outputSchemas.clone();
		this.schema = context.schema;
	}

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext() {
		this(0, 0, new DefaultFunctionRegistry(), new DefaultConstantRegistry());
	}

	/**
	 * Returns the {@link FunctionRegistry} containing all registered function in the current evaluation context.
	 * 
	 * @return the FunctionRegistry
	 */
	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.methodRegistry;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.RegistryScope#getConstantRegistry()
	 */
	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistry;
	}

	public int getInputCounter() {
		return this.inputCounter;
	}

	public void increaseInputCounter() {
		this.inputCounter++;
	}

	/**
	 * Returns the inputSchemas.
	 * 
	 * @return the inputSchemas
	 */
	public Schema getInputSchema(@SuppressWarnings("unused") final int index) {
		return this.schema;
		// return this.inputSchemas[index];
	}

	/**
	 * Returns the outputSchemas.
	 * 
	 * @return the outputSchemas
	 */
	public Schema getOutputSchema(@SuppressWarnings("unused") final int index) {
		return this.schema;
		// return this.outputSchemas[index];
	}

	private int taskId;

	public int getTaskId() {
		return this.taskId;
	}

	public void setTaskId(final int taskId) {
		this.taskId = taskId;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		builder.append("Context @ ").append(this.operatorStack).append("\n").
			append("Methods: ");
		this.methodRegistry.toString(builder);
		builder.append("\nConstants: ");
		this.constantRegistry.toString(builder);
	}

	/**
	 * @param schema
	 */
	public void setSchema(final Schema schema) {
		this.schema = schema;
	}
}
