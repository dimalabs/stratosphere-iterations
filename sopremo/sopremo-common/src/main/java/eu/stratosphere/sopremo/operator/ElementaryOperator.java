package eu.stratosphere.sopremo.operator;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.DegreeOfParallelism;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.IdentityList;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * An ElementaryOperator is an {@link Operator} that directly translates to a
 * PACT. Such an operator has at most one output.<br>
 * By convention, the first inner class of implementing operators that inherits
 * from {@link Stub} is assumed to be the implementation of this operator. The
 * following example demonstrates a minimalistic operator implementation.
 * 
 * <pre>
 * public static class TwoInputIntersection extends ElementaryOperator {
 * 	public TwoInputIntersection(JsonStream input1, JsonStream input2) {
 * 		super(input1, input2);
 * 	}
 * 
 * 	public static class Implementation extends
 * 			SopremoCoGroup&lt;PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject&gt; {
 * 		&#064;Override
 * 		public void coGroup(PactJsonObject.Key key,
 * 				Iterator&lt;PactJsonObject&gt; values1,
 * 				Iterator&lt;PactJsonObject&gt; values2,
 * 				Collector&lt;PactJsonObject.Key, PactJsonObject&gt; out) {
 * 			if (values1.hasNext() &amp;&amp; values2.hasNext())
 * 				out.collect(key, values1.next());
 * 		}
 * 	}
 * }
 * </pre>
 * 
 * To exert more control, several hooks are available that are called in fixed
 * order.
 * <ul>
 * <li>{@link #getStubClass()} allows to choose a different Stub than the first inner class inheriting from {@link Stub}.
 * <li>{@link #getContract()} instantiates a contract matching the stub class resulting from the previous callback. This
 * callback is especially useful if a PACT stub is chosen that is not supported in Sopremo yet.
 * <li>{@link #configureContract(Contract, Configuration, EvaluationContext)} is a callback used to set parameters of
 * the {@link Configuration} of the stub.
 * <li>{@link #asPactModule(EvaluationContext)} gives complete control over the creation of the {@link PactModule}.
 * </ul>
 * 
 * @author Arvid Heise
 */
@OutputCardinality(min = 1, max = 1)
public abstract class ElementaryOperator<Self extends ElementaryOperator<Self>>
		extends Operator<Self> {
	private static final org.apache.commons.logging.Log LOG = LogFactory
		.getLog(ElementaryOperator.class);

	private final List<List<? extends EvaluationExpression>> keyExpressions =
		new ArrayList<List<? extends EvaluationExpression>>();

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	@Property
	@Name(preposition = "into")
	public void setResultProjection(final EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		if (getMaxInputs() == 1)
			this.resultProjection = resultProjection.clone().remove(new InputSelection(0));
		else
			this.resultProjection = resultProjection;
	}

	public Self withResultProjection(final EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this.self();
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 4504792171699882490L;

	/**
	 * Initializes the ElementaryOperator with the number of outputs set to 1.
	 * The {@link InputCardinality} annotation must be set with this
	 * constructor.
	 */
	public ElementaryOperator() {
		super();
	}

	/**
	 * Initializes the ElementaryOperator with the given number of inputs.
	 * 
	 * @param minInputs
	 *        the minimum number of inputs
	 * @param maxInputs
	 *        the maximum number of inputs
	 */
	public ElementaryOperator(final int minInputs, final int maxInputs) {
		super(minInputs, maxInputs, 1, 1);
	}

	/**
	 * Initializes the ElementaryOperator with the given number of inputs.
	 * 
	 * @param inputs
	 *        the number of inputs
	 */
	public ElementaryOperator(final int inputs) {
		this(inputs, inputs);
	}

	{
		for (int index = 0; index < this.getMinInputs(); index++)
			this.keyExpressions.add(new ArrayList<EvaluationExpression>());
	}

	/**
	 * Returns the key expressions of the given input.
	 * 
	 * @param inputIndex
	 *        the index of the input
	 * @return the key expressions of the given input
	 */
	@SuppressWarnings("unchecked")
	public List<? extends EvaluationExpression> getKeyExpressions(
			final int inputIndex) {
		if (inputIndex >= this.keyExpressions.size())
			return Collections.EMPTY_LIST;
		final List<? extends EvaluationExpression> expressions = this.keyExpressions
			.get(inputIndex);
		if (expressions == null)
			return Collections.EMPTY_LIST;
		return expressions;
	}

	/**
	 * Sets the keyExpressions to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 */
	@Property(hidden = true)
	public void setKeyExpressions(final int inputIndex,
			final List<? extends EvaluationExpression> keyExpressions) {
		if (keyExpressions == null)
			throw new NullPointerException("keyExpressions must not be null");
		CollectionUtil.ensureSize(this.keyExpressions, inputIndex + 1);
		this.keyExpressions.set(inputIndex, keyExpressions);
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 */
	public void setKeyExpressions(final int index,
			final EvaluationExpression... keyExpressions) {
		if (keyExpressions.length == 0)
			throw new IllegalArgumentException(
				"keyExpressions must not be null");

		this.setKeyExpressions(index, Arrays.asList(keyExpressions));
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 * @return this
	 */
	public Self withKeyExpression(final int index,
			final EvaluationExpression... keyExpressions) {
		this.setKeyExpressions(index, keyExpressions);
		return this.self();
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 * @return this
	 */
	public Self withKeyExpressions(final int index,
			final List<? extends EvaluationExpression> keyExpressions) {
		this.setKeyExpressions(index, keyExpressions);
		return this.self();
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final Contract contract = this.getContract(context.getInputSchema(0));
		context.setResultProjection(this.resultProjection);
		this.configureContract(contract, contract.getParameters(), context);

		final List<List<Contract>> inputLists = ContractUtil
			.getInputs(contract);
		final List<Contract> distinctInputs = new IdentityList<Contract>();
		for (final List<Contract> inputs : inputLists) {
			// assume at least one input for each contract input slot
			if (inputs.isEmpty())
				inputs.add(MapContract.builder(IdentityMap.class).build());
			for (final Contract input : inputs)
				if (!distinctInputs.contains(input))
					distinctInputs.add(input);
		}
		final PactModule module = new PactModule(this.toString(),
			distinctInputs.size(), 1);
		for (final List<Contract> inputs : inputLists)
			for (int index = 0; index < inputs.size(); index++)
				inputs.set(index, module.getInput(distinctInputs.indexOf(inputs
					.get(index))));
		ContractUtil.setInputs(contract, inputLists);

		module.getOutput(0).addInput(contract);
		return module;
	}

	/**
	 * Creates a module that delegates all input directly to the output.
	 * 
	 * @return a short circuit module
	 */
	protected PactModule createShortCircuitModule() {
		final PactModule module = new PactModule("Short circuit", 1, 1);
		module.getOutput(0).setInput(module.getInput(0));
		return module;
	}

	/**
	 * Callback to add parameters to the stub configuration.<br>
	 * The default implementation adds the context and all non-transient,
	 * non-final, non-static fields.
	 * 
	 * @param contract
	 *        the contract to configure
	 * @param stubConfiguration
	 *        the configuration of the stub
	 * @param context
	 *        the context in which the {@link PactModule} is created and
	 *        evaluated
	 */
	protected void configureContract(final Contract contract,
			final Configuration stubConfiguration,
			final EvaluationContext context) {
		context.pushOperator(this);
		SopremoUtil.serialize(stubConfiguration, SopremoUtil.CONTEXT, context);
		context.popOperator();

		for (final Field stubField : contract.getUserCodeClass()
			.getDeclaredFields())
			if ((stubField.getModifiers() & (Modifier.TRANSIENT
				| Modifier.FINAL | Modifier.STATIC)) == 0) {
				Class<?> clazz = this.getClass();
				do {
					Field thisField;
					try {
						thisField = clazz.getDeclaredField(stubField.getName());
						thisField.setAccessible(true);
						SopremoUtil.serialize(stubConfiguration,
							stubField.getName(),
							(Serializable) thisField.get(this));
					} catch (final NoSuchFieldException e) {
						// ignore field of stub if the field does not exist in
						// this operator
					} catch (final Exception e) {
						LOG.error(String.format(
							"Could not serialize field %s of class %s: %s",
							stubField.getName(), contract.getClass(), e));
					}
				} while ((clazz = clazz.getSuperclass()) != ElementaryOperator.class);
			}

		final DegreeOfParallelism degreeOfParallelism = ReflectUtil
			.getAnnotation(this.getClass(), DegreeOfParallelism.class);
		if (degreeOfParallelism != null)
			contract.setDegreeOfParallelism(degreeOfParallelism.value());
	}

	@Override
	public ElementarySopremoModule asElementaryOperators(final EvaluationContext context) {
		final ElementarySopremoModule module =
			new ElementarySopremoModule(this.getName(), this.getInputs().size(), this.getOutputs()
				.size());
		final Operator<Self> clone = this.clone();
		for (int index = 0; index < this.getInputs().size(); index++)
			clone.setInput(index, module.getInput(index));
		final List<JsonStream> outputs = clone.getOutputs();
		for (int index = 0; index < outputs.size(); index++)
			module.getOutput(index).setInput(index, outputs.get(index));
		return module;
	}

	/**
	 * Creates the {@link Contract} that represents this operator.
	 * 
	 * @return the contract representing this operator
	 */
	@SuppressWarnings("unchecked")
	protected Contract getContract(final Schema globalSchema) {
		final Class<? extends Stub> stubClass = this.getStubClass();
		if (stubClass == null)
			throw new IllegalStateException("no implementing stub found");
		final Class<? extends Contract> contractClass = ContractUtil
			.getContractClass(stubClass);
		if (contractClass == null)
			throw new IllegalStateException("no associated contract found");

		try {
			if (contractClass == ReduceContract.class) {
				int[] keyIndices = this.getKeyIndices(globalSchema, this.getKeyExpressions(0));
				ReduceContract.Builder builder = ReduceContract.builder((Class<? extends ReduceStub>) stubClass);
				builder.name(this.toString());
				PactBuilderUtil.addKeys(builder, this.getKeyClasses(globalSchema, keyIndices), keyIndices);
				return builder.build();
			}
			else if (contractClass == CoGroupContract.class) {
				int[] keyIndices1 = this.getKeyIndices(globalSchema, this.getKeyExpressions(0));
				int[] keyIndices2 = this.getKeyIndices(globalSchema, this.getKeyExpressions(1));
				Class<? extends Key>[] keyTypes = this.getCommonKeyClasses(globalSchema, keyIndices1, keyIndices2);
				
				CoGroupContract.Builder builder = CoGroupContract.builder((Class<? extends CoGroupStub>) stubClass,
					keyTypes[0], keyIndices1[0], keyIndices2[0]);
				builder.name(this.toString());
				PactBuilderUtil.addKeysExceptFirst(builder, keyTypes, keyIndices1, keyIndices2);
				return builder.build();
			}
			else if (contractClass == MatchContract.class) {
				int[] keyIndices1 = this.getKeyIndices(globalSchema, this.getKeyExpressions(0));
				int[] keyIndices2 = this.getKeyIndices(globalSchema, this.getKeyExpressions(1));
				Class<? extends Key>[] keyTypes = this.getCommonKeyClasses(globalSchema, keyIndices1, keyIndices2);
				
				MatchContract.Builder builder = MatchContract.builder((Class<? extends MatchStub>) stubClass,
					keyTypes[0], keyIndices1[0], keyIndices2[0]);
				builder.name(this.toString());
				PactBuilderUtil.addKeysExceptFirst(builder, keyTypes, keyIndices1, keyIndices2);
				return builder.build();
			} else if(contractClass == MapContract.class) 
				return MapContract.builder((Class<? extends MapStub>) stubClass).
						name(this.toString()).build();
			else if(contractClass == CrossContract.class) 
				return CrossContract.builder((Class<? extends CrossStub>) stubClass).
						name(this.toString()).build();
			else throw new UnsupportedOperationException("Unknown contract type");
				
		} catch (final Exception e) {
			throw new IllegalStateException("Cannot create contract from stub "
				+ stubClass, e);
		}
	}

	private Class<? extends Key>[] getCommonKeyClasses(
			final Schema globalSchema, final int[] keyIndices1,
			final int[] keyIndices2) {
		final Class<? extends Key>[] keyClasses1 = this.getKeyClasses(
			globalSchema, keyIndices1);
		final Class<? extends Key>[] keyClasses2 = this.getKeyClasses(
			globalSchema, keyIndices2);
		if (!Arrays.equals(keyClasses1, keyClasses2))
			throw new IllegalStateException(
				String.format(
					"The key classes are not compatible (schema %s; indices %s %s; key classes: %s %s)",
					globalSchema, keyIndices1, keyIndices2,
					keyClasses1, keyClasses2));
		return keyClasses1;
	}

	// protected Iterable<? extends EvaluationExpression>
	// getKeyExpressionsForInput(final int index) {
	// final Iterable<? extends EvaluationExpression> keyExpressions =
	// this.getKeyExpressions();
	// if (keyExpressions == ALL_KEYS)
	// return keyExpressions;
	// return new FilteringIterable<EvaluationExpression>(keyExpressions, new
	// Predicate<EvaluationExpression>() {
	// @Override
	// public boolean isTrue(EvaluationExpression expression) {
	// return SopremoUtil.getInputIndex(expression) == index;
	// };
	// });
	// }
	//
	public List<List<? extends EvaluationExpression>> getAllKeyExpressions() {
		final ArrayList<List<? extends EvaluationExpression>> allKeys =
			new ArrayList<List<? extends EvaluationExpression>>();
		final List<JsonStream> inputs = this.getInputs();
		for (int index = 0; index < inputs.size(); index++)
			allKeys.add(this.getKeyExpressions(index));
		return allKeys;
	}

	@SuppressWarnings("unchecked")
	private Class<? extends Key>[] getKeyClasses(final Schema globalSchema,
			final int[] keyIndices) {
		final Class<? extends Value>[] pactSchema = globalSchema
			.getPactSchema();
		final Class<? extends Key>[] keyClasses = new Class[keyIndices.length];
		for (int index = 0; index < keyIndices.length; index++) {
			final Class<? extends Value> schemaClass = pactSchema[keyIndices[index]];
			if (!Key.class.isAssignableFrom(schemaClass))
				throw new IllegalStateException(
					"Schema wrapped a key value with a class that does not implement Key");
			keyClasses[index] = (Class<? extends Key>) schemaClass;
		}
		return keyClasses;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.keyExpressions.hashCode();
		result = prime * result + this.resultProjection.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final ElementaryOperator<?> other = (ElementaryOperator<?>) obj;
		return this.keyExpressions.equals(other.keyExpressions) && this.resultProjection.equals(other.resultProjection);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		if (this.getResultProjection() != EvaluationExpression.VALUE)
			builder.append(" to ").append(this.getResultProjection());
		return builder.toString();
	}

	private int[] getKeyIndices(final Schema globalSchema,
			final Iterable<? extends EvaluationExpression> keyExpressions) {
		if (keyExpressions.equals(ALL_KEYS)) {
			final int[] allSchema = new int[globalSchema.getPactSchema().length];
			for (int index = 0; index < allSchema.length; index++)
				allSchema[index] = index;
			return allSchema;
		}
		final IntSet keyIndices = new IntOpenHashSet();
		for (final EvaluationExpression expression : keyExpressions)
			keyIndices.addAll(globalSchema.indicesOf(expression));
		if (keyIndices.isEmpty()) {
			if (keyExpressions.iterator().hasNext())
				throw new IllegalStateException(
					String.format(
						"Operator %s did not specify key expression that it now requires",
						this.getClass()));

			throw new IllegalStateException(String.format(
				"Needs to specify key expressions: %s", this.getClass()));
		}
		return keyIndices.toIntArray();
	}

	// protected abstract Schema getKeyFields();

	/**
	 * Returns the stub class that represents the functionality of this
	 * operator.<br>
	 * This method returns the first static inner class found with {@link Class#getDeclaredClasses()} that is extended
	 * from {@link Stub} by
	 * default.
	 * 
	 * @return the stub class
	 */
	@SuppressWarnings("unchecked")
	protected Class<? extends Stub> getStubClass() {
		for (final Class<?> stubClass : this.getClass().getDeclaredClasses())
			if ((stubClass.getModifiers() & Modifier.STATIC) != 0
				&& Stub.class.isAssignableFrom(stubClass))
				return (Class<? extends Stub>) stubClass;
		return null;
	}
}
