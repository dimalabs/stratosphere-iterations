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

package eu.stratosphere.pact.runtime.task.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.iterative.convergence.ConvergenceCriterion;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;

/**
 * Configuration class which stores all relevant parameters required to set up the Pact tasks.
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class TaskConfig
{
	/**
	 * Enumeration of all available local strategies for Pact tasks. 
	 */
	public enum LocalStrategy {
		// both inputs are sorted and merged
		SORT_BOTH_MERGE,
		// the first input is sorted and merged with the (already sorted) second input
		SORT_FIRST_MERGE,
		// the second input is sorted and merged with the (already sorted) first input
		SORT_SECOND_MERGE,
		// both (already sorted) inputs are merged
		MERGE,
		// input is sorted, within a key values are crossed in a nested loop fashion
		SORT_SELF_NESTEDLOOP,
		// already grouped input, within a key values are crossed in a nested loop fashion
		SELF_NESTEDLOOP,
		// the input is sorted
		SORT,
		// the input is sorted, during sorting a combiner is applied
		COMBININGSORT,
		// the first input is build side, the second side is probe side of a hybrid hash table
		HYBRIDHASH_FIRST,
		// the second input is build side, the first side is probe side of a hybrid hash table
		HYBRIDHASH_SECOND,
		// the first input is build side, the second side is probe side of an in-memory hash table
		MMHASH_FIRST,
		// the second input is build side, the first side is probe side of an in-memory hash table
		MMHASH_SECOND,
		// the second input is inner loop, the first input is outer loop and block-wise processed
		NESTEDLOOP_BLOCKED_OUTER_FIRST,
		// the first input is inner loop, the second input is outer loop and block-wise processed
		NESTEDLOOP_BLOCKED_OUTER_SECOND,
		// the second input is inner loop, the first input is outer loop and stream-processed
		NESTEDLOOP_STREAMED_OUTER_FIRST,
		// the first input is inner loop, the second input is outer loop and stream-processed
		NESTEDLOOP_STREAMED_OUTER_SECOND,
		// no special local strategy is applied
		NONE
	}
	
	// --------------------------------------------------------------------------------------------

	private static final String DRIVER_CLASS = "pact.driver.class";
	
	private static final String STUB_CLASS = "pact.stub.class";

	private static final String STUB_PARAM_PREFIX = "pact.stub.param.";
	
	private static final String LOCAL_STRATEGY = "pact.local.strategy";

	private static final String NUM_OUTPUTS = "pact.outputs.num";
	
	private static final String OUTPUT_SHIP_STRATEGY_PREFIX = "pact.out.shipstrategy.";
	
	private static final String OUTPUT_TYPE_SERIALIZER_FACTORY = "pact.out.serializer";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_CLASS = "pact.out.distribution.class";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_STATE = "pact.out.distribution.state";
	
	private static final String OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX = "pact.out.comparator.";
	
	private static final String OUTPUT_PARAMETERS_PREFIX = "pact.out.param.";
	
	private static final String INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "pact.in.serializer.";
	
	private static final String INPUT_TYPE_COMPARATOR_FACTORY_PREFIX = "pact.in.comparator.";
	
	private static final String INPUT_PARAMETERS_PREFIX = "pact.in.param.";
	
	private static final String INPUT_PAIR_COMPARATOR_FACTORY = "pact.in.paircomp";
	
	/*
	 * If one input has multiple predecessors (bag union), multiple
	 * inputs must be grouped together. For a map or reduce there is
	 * one group and "pact.size.inputGroup.1" will be equal to
	 * "pact.inputs.number"
	 * 
	 * In the case of a dual input pact (eg. match) there might be
	 * 2 predecessors for the first group and one for the second group.
	 * Hence, "pact.inputs.number" would be 3, "pact.size.inputGroup.1"
	 * would be 2, and "pact.size.inputGroup.2" would be 1.
	 */
	private static final String INPUT_GROUP_SIZE_PREFIX = "pact.size.inputGroup.";
	
	private static final String NUM_INPUTS = "pact.inputs.number";
	
	private static final String CHAINING_NUM_STUBS = "pact.chaining.num";
	
	private static final String CHAINING_TASKCONFIG_PREFIX = "pact.chaining.taskconfig.";
	
	private static final String CHAINING_TASK_PREFIX = "pact.chaining.task.";
	
	private static final String CHAINING_TASKNAME_PREFIX = "pact.chaining.taskname.";
	
	private static final String SIZE_MEMORY = "pact.memory.size";

	private static final String NUM_FILEHANDLES = "pact.filehandles.num";
	
	private static final String SORT_SPILLING_THRESHOLD = "pact.sort.spillthreshold";

	// --------------------------------------------------------------------------------------------

  private static final String NUMBER_OF_EVENTS_UNTIL_INTERRUPT = "pact.iterative.numberOfEventsUntilInterrupt.";

  private static final String INPUT_GATE_CACHED = "pact.iterative.inputGateCached.";

  private static final String INPUT_GATE_CACHE_MEMORY_FRACTION = "pact.iterative.inputGateCacheMemoryFraction";

  private static final String NUMBER_OF_ITERATIONS = "pact.iterative.numberOfIterations";

  private static final String BACKCHANNEL_MEMORY_FRACTION = "pact.iterative.backChannelMemoryFraction";

  private static final String CONVERGENCE_CRITERION = "pact.iterative.terminationCriterion";

  // --------------------------------------------------------------------------------------------

	protected final Configuration config;			// the actual configuration holding the values

	
	public TaskConfig(Configuration config)
	{
		this.config = config;
	}
	
	public Configuration getConfiguration() {
		return this.config;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Pact Driver
	// --------------------------------------------------------------------------------------------
	
	public void setDriver(@SuppressWarnings("rawtypes") Class<? extends PactDriver> driver) {
		this.config.setString(DRIVER_CLASS, driver.getName());
	}
	
	public <S extends Stub, OT> Class<? extends PactDriver<S, OT>> getDriver()
	{
		final String className = this.config.getString(DRIVER_CLASS, null);
		if (className == null) {
			throw new CorruptConfigurationException("The pact driver class is missing.");
		}
		
		try {
			@SuppressWarnings("unchecked")
			final Class<PactDriver<S, OT>> pdClazz = (Class<PactDriver<S, OT>>) (Class<?>) PactDriver.class;
			return Class.forName(className).asSubclass(pdClazz);
		} catch (ClassNotFoundException cnfex) {
			throw new CorruptConfigurationException("The given driver class cannot be found.");
		} catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The given driver class does not implement the pact driver interface.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                User code class Access
	// --------------------------------------------------------------------------------------------

	public void setStubClass(Class<?> stubClass) {
		this.config.setString(STUB_CLASS, stubClass.getName());
	}

	public <T> Class<? extends T> getStubClass(Class<T> stubClass, ClassLoader cl)
		throws ClassNotFoundException, ClassCastException
	{
		final String stubClassName = this.config.getString(STUB_CLASS, null);
		if (stubClassName == null) {
			throw new CorruptConfigurationException("The stub class is missing.");
		}
		return Class.forName(stubClassName, true, cl).asSubclass(stubClass);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                User Code Parameters
	// --------------------------------------------------------------------------------------------

	public void setStubParameters(Configuration parameters)
	{
		this.config.addAll(parameters, STUB_PARAM_PREFIX);
	}

	public Configuration getStubParameters()
	{
		return new DelegatingConfiguration(this.config, STUB_PARAM_PREFIX);
	}
	
	public void setStubParameter(String key, String value)
	{
		this.config.setString(STUB_PARAM_PREFIX + key, value);
	}

	public String getStubParameter(String key, String defaultValue)
	{
		return this.config.getString(STUB_PARAM_PREFIX + key, defaultValue);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                 Local Strategies
	// --------------------------------------------------------------------------------------------
	
	public void setLocalStrategy(LocalStrategy strategy) {
		this.config.setInteger(LOCAL_STRATEGY, strategy.ordinal());
	}

	public LocalStrategy getLocalStrategy()
	{
		final int ls = this.config.getInteger(LOCAL_STRATEGY, -1);
		if (ls == -1) {
			return LocalStrategy.NONE;
		} else if (ls < 0 || ls >= LocalStrategy.values().length) {
			throw new CorruptConfigurationException("Illegal local strategy in configuration: " + ls);
		} else {
			return LocalStrategy.values()[ls];
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                               Inputs and dependent Parameters
	// --------------------------------------------------------------------------------------------
	
	public void setSerializerFactoryForInput(Class<? extends TypeSerializerFactory<?>> clazz, int inputNum)
	{
		this.config.setString(INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum, clazz.getName());
	}
	
	public void setComparatorFactoryForInput(Class<? extends TypeComparatorFactory<?>> clazz, int inputNum)
	{
		this.config.setString(INPUT_TYPE_COMPARATOR_FACTORY_PREFIX + inputNum, clazz.getName());
	}
	
	public void setPairComparatorFactory(Class<? extends TypePairComparatorFactory<?, ?>> clazz)
	{
		this.config.setString(INPUT_PAIR_COMPARATOR_FACTORY, clazz.getName());
	}
	
	public <T> Class<? extends TypeSerializerFactory<T>> getSerializerFactoryForInput(int inputNum, ClassLoader cl)
	throws ClassNotFoundException
	{
		final String className = this.config.getString(INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum, null);
		if (className == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final Class<TypeSerializerFactory<T>> superClass = (Class<TypeSerializerFactory<T>>) (Class<?>) TypeSerializerFactory.class;
			try {
				return Class.forName(className, true, cl).asSubclass(superClass);
			} catch (ClassCastException ccex) {
				throw new CorruptConfigurationException("The class noted in the configuration as the serializer factory " +
						"is no subclass of TypeSerializerFactory.");
			}
		}
	}
	
	public <T> Class<? extends TypeComparatorFactory<T>> getComparatorFactoryForInput(int inputNum, ClassLoader cl)
	throws ClassNotFoundException
	{
		final String className = this.config.getString(INPUT_TYPE_COMPARATOR_FACTORY_PREFIX + inputNum, null);
		if (className == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final Class<TypeComparatorFactory<T>> superClass = (Class<TypeComparatorFactory<T>>) (Class<?>) TypeComparatorFactory.class;
			try {
				return Class.forName(className, true, cl).asSubclass(superClass);
			} catch (ClassCastException ccex) {
				throw new CorruptConfigurationException("The class noted in the configuration as the comparator factory " +
						"is no subclass of TypeComparatorFactory.");
			}
		}
	}
	
	public <T1, T2> Class<? extends TypePairComparatorFactory<T1, T2>> getPairComparatorFactory(ClassLoader cl)
		throws ClassNotFoundException
		{
			final String className = this.config.getString(INPUT_PAIR_COMPARATOR_FACTORY, null);
			if (className == null) {
				return null;
			} else {
				@SuppressWarnings("unchecked")
				final Class<TypePairComparatorFactory<T1, T2>> superClass = (Class<TypePairComparatorFactory<T1, T2>>) (Class<?>) TypePairComparatorFactory.class;
				try {
					return Class.forName(className, true, cl).asSubclass(superClass);
				} catch (ClassCastException ccex) {
					throw new CorruptConfigurationException("The class noted in the configuration as the pair comparator factory " +
							"is no subclass of TypePairComparatorFactory.");
				}
			}
		}
	
	public Configuration getConfigForInputParameters(int inputNum)
	{
		return new DelegatingConfiguration(this.config, INPUT_PARAMETERS_PREFIX + inputNum + '.');
	}
	
	// --------------------------------------------------------------------------------------------
	//                          Parameters for the output shipping
	// --------------------------------------------------------------------------------------------

	public void addOutputShipStrategy(ShipStrategyType strategy)
	{
		final int outputCnt = this.config.getInteger(NUM_OUTPUTS, 0);
		this.config.setInteger(OUTPUT_SHIP_STRATEGY_PREFIX + outputCnt, strategy.ordinal());
		this.config.setInteger(NUM_OUTPUTS, outputCnt + 1);
	}
	
	public int getNumOutputs()
	{
		return this.config.getInteger(NUM_OUTPUTS, -1);
	}

	public ShipStrategyType getOutputShipStrategy(int outputId)
	{
		// check how many outputs are encoded in the config
		final int outputCnt = this.config.getInteger(NUM_OUTPUTS, -1);
		if (outputCnt < 1) {
			throw new CorruptConfigurationException("No output ship strategies are specified in the configuration.");
		}
		
		// sanity range checks
		if (outputId < 0 || outputId >= outputCnt) {
			throw new IllegalArgumentException("Invalid index for output shipping strategy.");
		}
		
		final int strategy = this.config.getInteger(OUTPUT_SHIP_STRATEGY_PREFIX + outputId, -1);
		if (strategy == -1) {
			throw new CorruptConfigurationException("No output shipping strategy in configuration for output "
																			+ outputId);
		} else if (strategy < 0 || strategy >= ShipStrategyType.values().length) {
			throw new CorruptConfigurationException("Illegal output shipping strategy in configuration for output "
																			+ outputId + ": " + strategy);
		} else {
			return ShipStrategyType.values()[strategy];
		}
	}
	
	public void setSerializerFactoryForOutput(Class<? extends TypeSerializerFactory<?>> clazz)
	{
		this.config.setString(OUTPUT_TYPE_SERIALIZER_FACTORY, clazz.getName());
	}
	
	public void setComparatorFactoryForOutput(Class<? extends TypeComparatorFactory<?>> clazz, int outputNum)
	{
		this.config.setString(OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum, clazz.getName());
	}
	
	public <T> Class<? extends TypeSerializerFactory<T>> getSerializerFactoryForOutput(ClassLoader cl)
		throws ClassNotFoundException
	{
		final String className = this.config.getString(OUTPUT_TYPE_SERIALIZER_FACTORY, null);
		if (className == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final Class<TypeSerializerFactory<T>> superClass = (Class<TypeSerializerFactory<T>>) (Class<?>) TypeSerializerFactory.class;
			try {
				return Class.forName(className, true, cl).asSubclass(superClass);
			} catch (ClassCastException ccex) {
				throw new CorruptConfigurationException("The class noted in the configuration as the serializer factory " +
						"is no subclass of TypeSerializerFactory.");
			}
		}
	}
		
	public <T> Class<? extends TypeComparatorFactory<T>> getComparatorFactoryForOutput(int outputNum, ClassLoader cl)
		throws ClassNotFoundException
	{
		final String className = this.config.getString(OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum, null);
		if (className == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final Class<TypeComparatorFactory<T>> superClass = (Class<TypeComparatorFactory<T>>) (Class<?>) TypeComparatorFactory.class;
			try {
				return Class.forName(className, true, cl).asSubclass(superClass);
			} catch (ClassCastException ccex) {
				throw new CorruptConfigurationException("The class noted in the configuration as the comparator factory " +
						"is no subclass of TypeComparatorFactory.");
			}
		}
	}
	
	public Configuration getConfigForOutputParameters(int outputNum)
	{
		return new DelegatingConfiguration(this.config, OUTPUT_PARAMETERS_PREFIX + outputNum + '.');
	}
	
	public String getPrefixForOutputParameters(int outputNum)
	{
		return OUTPUT_PARAMETERS_PREFIX + outputNum + '.';
	}
	
	public void setOutputDataDistribution(DataDistribution distribution)
	{
		this.config.setString(OUTPUT_DATA_DISTRIBUTION_CLASS, distribution.getClass().getName());
		
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);
		try {
			distribution.write(dos);
		} catch (IOException e) {
			throw new RuntimeException("Error serializing the DataDistribution: " + e.getMessage(), e);
		}

		this.config.setBytes(OUTPUT_DATA_DISTRIBUTION_STATE, baos.toByteArray());
	}
	
	public DataDistribution getOutputDataDistribution(final ClassLoader cl) throws ClassNotFoundException
	{
		final String className = this.config.getString(OUTPUT_DATA_DISTRIBUTION_CLASS, null);
		if (className == null) {
			return null;
		}
		
		final Class<? extends DataDistribution> clazz;
		try {
			clazz = Class.forName(className, true, cl).asSubclass(DataDistribution.class);
		} catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the data distribution " +
					"is no subclass of DataDistribution.");
		}
		
		final DataDistribution distribution = InstantiationUtil.instantiate(clazz, DataDistribution.class);
		
		final byte[] stateEncoded = this.config.getBytes(OUTPUT_DATA_DISTRIBUTION_STATE, null);
		if (stateEncoded == null) {
			throw new CorruptConfigurationException(
						"The configuration contained the data distribution type, but no serialized state.");
		}
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(stateEncoded);
		final DataInputStream in = new DataInputStream(bais);
		
		try {
			distribution.read(in);
			return distribution;
		} catch (Exception ex) {
			throw new RuntimeException("The deserialization of the encoded data distribution state caused an error"
				+ ex.getMessage() == null ? "." : ": " + ex.getMessage(), ex);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------

	public int getNumInputs() {
		return config.getInteger(NUM_INPUTS, -1);
	}
	
	public int getGroupSize(int groupIndex) {
		return this.config.getInteger(INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	
	public void addInputToGroup(int groupIndex) {
		String grp = INPUT_GROUP_SIZE_PREFIX + groupIndex;
		this.config.setInteger(grp, this.config.getInteger(grp, 0)+1);
		this.config.setInteger(NUM_INPUTS, this.config.getInteger(NUM_INPUTS, 0)+1);
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the amount of memory dedicated to the task's input preparation (sorting / hashing).
	 * 
	 * @param memSize The memory size in bytes.
	 */
	public void setMemorySize(long memorySize) {
		this.config.setLong(SIZE_MEMORY, memorySize);
	}

	/**
	 * Sets the maximum number of open files.
	 * 
	 * @param numFileHandles Maximum number of open files.
	 */
	public void setNumFilehandles(int numFileHandles) {
		if (numFileHandles < 2) {
			throw new IllegalArgumentException();
		}
		
		this.config.setInteger(NUM_FILEHANDLES, numFileHandles);
	}
	
	/**
	 * Sets the threshold that triggers spilling to disk of intermediate sorted results. This value defines the
	 * fraction of the buffers that must be full before the spilling is triggered.
	 * 
	 * @param threshold The value for the threshold.
	 */
	public void setSortSpillingTreshold(float threshold) {
		if (threshold < 0.0f || threshold > 1.0f) {
			throw new IllegalArgumentException();
		}
		
		this.config.setFloat(SORT_SPILLING_THRESHOLD, threshold);
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the amount of memory dedicated to the task's input preparation (sorting / hashing).
	 * Returns <tt>-1</tt> if the value is not specified.
	 * 
	 * @return The memory size in bytes.
	 */
	public long getMemorySize() {
		return this.config.getLong(SIZE_MEMORY, -1);
	}

	/**
	 * Gets the maximum number of open files. Returns <tt>-1</tt>, if the value has not been set.
	 * 
	 * @return Maximum number of open files.
	 */
	public int getNumFilehandles() {
		return this.config.getInteger(NUM_FILEHANDLES, -1);
	}
	
	/**
	 * Gets the threshold that triggers spilling to disk of intermediate sorted results. This value defines the
	 * fraction of the buffers that must be full before the spilling is triggered.
	 * <p>
	 * If the value is not set, this method returns a default value if <code>0.7f</code>.
	 * 
	 * @return The threshold that triggers spilling to disk of sorted intermediate results.
	 */
	public float getSortSpillingTreshold() {
		return this.config.getFloat(SORT_SPILLING_THRESHOLD, 0.7f);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Parameters for Stub Chaining
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfChainedStubs() {
		return this.config.getInteger(CHAINING_NUM_STUBS, 0);
	}
	
	public void addChainedTask(@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> chainedTaskClass, TaskConfig conf, String taskName)
	{
		int numChainedYet = this.config.getInteger(CHAINING_NUM_STUBS, 0);
		
		this.config.setString(CHAINING_TASK_PREFIX + numChainedYet, chainedTaskClass.getName());
		this.config.addAll(conf.config, CHAINING_TASKCONFIG_PREFIX + numChainedYet + '.');
		this.config.setString(CHAINING_TASKNAME_PREFIX + numChainedYet, taskName);
		
		this.config.setInteger(CHAINING_NUM_STUBS, ++numChainedYet);
	}
	
	public TaskConfig getChainedStubConfig(int chainPos)
	{
		return new TaskConfig(new DelegatingConfiguration(this.config, CHAINING_TASKCONFIG_PREFIX + chainPos + '.'));
	}

	public Class<? extends ChainedDriver<?, ?>> getChainedTask(int chainPos)
	throws ClassNotFoundException, ClassCastException
	{
		String className = this.config.getString(CHAINING_TASK_PREFIX + chainPos, null);
		if (className == null)
			throw new IllegalStateException("Chained Task Class missing");
		
		@SuppressWarnings("unchecked")
		final Class<ChainedDriver<?, ?>> clazz = (Class<ChainedDriver<?, ?>>) (Class<?>) ChainedDriver.class;
		return Class.forName(className).asSubclass(clazz);
	}
	
	public String getChainedTaskName(int chainPos)
	{
		return this.config.getString(CHAINING_TASKNAME_PREFIX + chainPos, null);
	}

	// --------------------------------------------------------------------------------------------
	// Parameters for iterations
	// --------------------------------------------------------------------------------------------

  public void setBackChannelMemoryFraction(float fraction) {
    Preconditions.checkArgument(fraction > 0 && fraction < 1);
    config.setFloat(BACKCHANNEL_MEMORY_FRACTION, fraction);
  }

  public float getBackChannelMemoryFraction() {
    float backChannelMemoryFraction = config.getFloat(BACKCHANNEL_MEMORY_FRACTION, 0);
    Preconditions.checkState(backChannelMemoryFraction > 0);
    return backChannelMemoryFraction;
  }

  public void setNumberOfIterations(int numberOfIterations) {
    Preconditions.checkArgument(numberOfIterations > 0);
    config.setInteger(NUMBER_OF_ITERATIONS, numberOfIterations);
  }

  public int getNumberOfIterations() {
    int numberOfIterations = config.getInteger(NUMBER_OF_ITERATIONS, 0);
    Preconditions.checkState(numberOfIterations > 0);
    return numberOfIterations;
  }

  public boolean isCachedInputGate(int inputGateIndex) {
    return config.getBoolean(INPUT_GATE_CACHED + inputGateIndex, false);
  }

  public void setGateCached(int inputGateIndex) {
    config.setBoolean(INPUT_GATE_CACHED + inputGateIndex, true);
  }

  public float getInputGateCacheMemoryFraction() {
    float inputGateCacheMemoryFraction = config.getFloat(INPUT_GATE_CACHE_MEMORY_FRACTION, 0);
    Preconditions.checkState(inputGateCacheMemoryFraction > 0);
    return inputGateCacheMemoryFraction;
  }

  public void setInputGateCacheMemoryFraction(float fraction) {
    Preconditions.checkArgument(fraction > 0 && fraction < 1);
    config.setFloat(INPUT_GATE_CACHE_MEMORY_FRACTION, fraction);
  }

  public boolean isIterativeInputGate(int inputGateIndex) {
    return getNumberOfEventsUntilInterruptInIterativeGate(inputGateIndex) > 0;
  }

  public void setGateIterativeWithNumberOfEventsUntilInterrupt(int inputGateIndex, int numEvents) {
    Preconditions.checkArgument(numEvents > 0);
    config.setInteger(NUMBER_OF_EVENTS_UNTIL_INTERRUPT + inputGateIndex, numEvents);
  }

  public int getNumberOfEventsUntilInterruptInIterativeGate(int inputGateIndex) {
    return config.getInteger(NUMBER_OF_EVENTS_UNTIL_INTERRUPT + inputGateIndex, 0);
  }

  public void setConvergenceCriterion(Class<? extends ConvergenceCriterion> convergenceCriterionClass) {
    config.setClass(CONVERGENCE_CRITERION, convergenceCriterionClass);
  }

  public Class<? extends ConvergenceCriterion> getConvergenceCriterion() {
    return Preconditions.checkNotNull(config.getClass(CONVERGENCE_CRITERION, null, ConvergenceCriterion.class));
  }

  public boolean usesConvergenceCriterion() {
    return config.getClass(CONVERGENCE_CRITERION, null) != null;
  }
	
	// --------------------------------------------------------------------------------------------
	//                              Utility class for nested Configurations
	// --------------------------------------------------------------------------------------------
	
	public static final class DelegatingConfiguration extends Configuration
	{
		private final Configuration backingConfig;		// the configuration actually storing the data
		
		private String prefix;							// the prefix key by which keys for this config are marked
		
		// --------------------------------------------------------------------------------------------
		
		/**
		 * Default constructor for serialization. Creates an empty delegating configuration.
		 */
		public DelegatingConfiguration()
		{
			this.backingConfig = new Configuration();
		}
		
		/**
		 * Creates a new delegating configuration which stores its key/value pairs in the given
		 * configuration using the specifies key prefix.
		 * 
		 * @param backingConfig The configuration holding the actual config data.
		 * @param prefix The prefix prepended to all config keys.
		 */
		public DelegatingConfiguration(Configuration backingConfig, String prefix)
		{
			this.backingConfig = backingConfig;
			this.prefix = prefix;
		}

		// --------------------------------------------------------------------------------------------
		
		@Override
		public String getString(String key, String defaultValue) {
			return this.backingConfig.getString(this.prefix + key, defaultValue);
		}

		@Override
		public ClassLoader getClassLoader() {
			return this.backingConfig.getClassLoader();
		}

		@Override
		public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, Class<T> ancestor) {
			return this.backingConfig.getClass(this.prefix + key, defaultValue, ancestor);
		}

		@Override
		public Class<?> getClass(String key, Class<?> defaultValue) {
			return this.backingConfig.getClass(this.prefix + key, defaultValue);
		}

		@Override
		public void setClass(String key, Class<?> klazz) {
			this.backingConfig.setClass(this.prefix + key, klazz);
		}

		@Override
		public void setString(String key, String value) {
			this.backingConfig.setString(this.prefix + key, value);
		}

		@Override
		public int getInteger(String key, int defaultValue) {
			return this.backingConfig.getInteger(this.prefix + key, defaultValue);
		}

		@Override
		public void setInteger(String key, int value) {
			this.backingConfig.setInteger(this.prefix + key, value);
		}

		@Override
		public long getLong(String key, long defaultValue) {
			return this.backingConfig.getLong(this.prefix + key, defaultValue);
		}

		@Override
		public void setLong(String key, long value) {
			this.backingConfig.setLong(this.prefix + key, value);
		}

		@Override
		public boolean getBoolean(String key, boolean defaultValue) {
			return this.backingConfig.getBoolean(this.prefix + key, defaultValue);
		}

		@Override
		public void setBoolean(String key, boolean value) {
			this.backingConfig.setBoolean(this.prefix + key, value);
		}

		@Override
		public float getFloat(String key, float defaultValue) {
			return this.backingConfig.getFloat(this.prefix + key, defaultValue);
		}

		@Override
		public void setFloat(String key, float value) {
			this.backingConfig.setFloat(this.prefix + key, value);
		}

		@Override
		public Set<String> keySet()
		{
			final HashSet<String> set = new HashSet<String>();
			final int prefixLen = this.prefix == null ? 0 : this.prefix.length();
			
			for (String key : this.backingConfig.keySet()) {
				if (key.startsWith(this.prefix)) {
					set.add(key.substring(prefixLen));
				}
			}
			return set;
		}
		
		// --------------------------------------------------------------------------------------------

		@Override
		public void read(DataInput in) throws IOException
		{
			this.prefix = in.readUTF();
			this.backingConfig.read(in);
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(this.prefix);
			this.backingConfig.write(out);
		}
		
		// --------------------------------------------------------------------------------------------

		@Override
		public int hashCode()
		{
			return this.prefix.hashCode() ^ this.backingConfig.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj instanceof DelegatingConfiguration) {
				DelegatingConfiguration other = (DelegatingConfiguration) obj;
				return this.prefix.equals(other.prefix) && this.backingConfig.equals(other.backingConfig);
			}
			else return false;
		}
	}
}
