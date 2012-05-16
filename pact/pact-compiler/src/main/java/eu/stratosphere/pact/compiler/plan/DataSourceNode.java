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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputSchemaProvider;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a data source.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DataSourceNode extends OptimizerNode
{
	private List<OptimizerNode> cachedPlans; // the cache in case there are multiple outputs;

	/**
	 * Creates a new DataSourceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data source contract object.
	 */
	public DataSourceNode(GenericDataSource<?> pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy of the data-source object for the process of plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param gp
	 *        The global properties of this copy.
	 * @param lp
	 *        The local properties of this copy.
	 */
	protected DataSourceNode(DataSourceNode template, GlobalProperties gp, LocalProperties lp) {
		super(template, gp, lp);
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericDataSource<?> getPactContract() {
		return (GenericDataSource<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Data Source";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<List<PactConnection>> getIncomingConnections() {
		return Collections.<List<PactConnection>>emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException("A DataSourceNode does not have any input.");
	}

	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the file size and the compiler hints. The compiler hints are instantiated with
	 * conservative default values which are used if no other values are provided.
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics)
	{
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// for unique keys, we can have only one value per key
//		OutputContract oc = getOutputContract();
//		if (oc == OutputContract.UniqueKey) {
//			hints.setAvgNumValuesPerKey(1.0f);
//		}
		
		// initialize basic estimates to unknown
		this.estimatedOutputSize = -1;
		this.estimatedNumRecords = -1;

		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null)
		{
			// instantiate the input format, as this is needed by the statistics 
			InputFormat<?, ?> format = null;
			String inFormatDescription = "<unknown>";
			
			try {
				Class<? extends InputFormat<?, ?>> formatClass = getPactContract().getFormatClass();
				format = formatClass.newInstance();
				format.configure(getPactContract().getParameters());
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled())
					PactCompiler.LOG.warn("Could not instantiate input format to obtain statistics."
						+ " Limited statistics will be available.", t);
				return;
			}
			try {
				inFormatDescription = format.toString();
			}
			catch (Throwable t) {}
			
			// first of all, get the statistics from the cache
			final String statisticsKey = getPactContract().getParameters().getString(InputFormat.STATISTICS_CACHE_KEY, null);
			final BaseStatistics cachedStatistics = statistics.getBaseStatistics(statisticsKey);
			
			BaseStatistics bs = null;
			try {
				bs = format.getStatistics(cachedStatistics);
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled())
					PactCompiler.LOG.warn("Error obtaining statistics from input format: " + t.getMessage(), t);
			}
			
			if (bs != null) {
				final long len = bs.getTotalInputSize();
				if (len == BaseStatistics.UNKNOWN) {
					if (PactCompiler.LOG.isWarnEnabled())
						PactCompiler.LOG.warn("Pact compiler could not determine the size of input '" + inFormatDescription + "'.");
				}
				else if (len >= 0) {
					this.estimatedOutputSize = len;
				}

				final float avgBytes = bs.getAverageRecordWidth();
				if (avgBytes > 0.0f && hints.getAvgBytesPerRecord() < 1.0f) {
					hints.setAvgBytesPerRecord(avgBytes);
				}
				
				final long card = bs.getNumberOfRecords();
				if (card != BaseStatistics.UNKNOWN) {
					this.estimatedNumRecords = card;
				}
			}
		}

		// the estimated number of rows is depending on the average row width
		if (this.estimatedNumRecords == -1 && hints.getAvgBytesPerRecord() >= 1.0f && this.estimatedOutputSize > 0) {
			this.estimatedNumRecords = (long) (this.estimatedOutputSize / hints.getAvgBytesPerRecord()) + 1;
		}

		// the key cardinality is either explicitly specified, derived from an avgNumValuesPerKey hint, 
		// or we assume for robustness reasons that every record has a unique key. 
		// Key cardinality overestimation results in more robust plans
		
		this.estimatedCardinality.putAll(hints.getDistinctCounts());
		
		if(this.estimatedNumRecords != -1) {
			for (Entry<FieldSet, Float> avgNumValues : hints.getAvgNumRecordsPerDistinctFields().entrySet()) {
				if (estimatedCardinality.get(avgNumValues.getKey()) == null) {
					long estimatedCard = (this.estimatedNumRecords / avgNumValues.getValue() >= 1) ? 
							(long) (this.estimatedNumRecords / avgNumValues.getValue()) : 1;
					estimatedCardinality.put(avgNumValues.getKey(), estimatedCard);
				}
			}
		}
		else {
			// if we have the key cardinality and an average number of values per key, we can estimate the number
			// of rows
			this.estimatedNumRecords = 0;
			int count = 0;
			
			for (Entry<FieldSet, Long> cardinality : hints.getDistinctCounts().entrySet()) {
				float avgNumValues = hints.getAvgNumRecordsPerDistinctFields(cardinality.getKey());
				if (avgNumValues != -1) {
					this.estimatedNumRecords += cardinality.getValue() * avgNumValues;
					count++;
				}
			}
			
			if (count > 0) {
				this.estimatedNumRecords = (this.estimatedNumRecords /count) >= 1 ?
						(this.estimatedNumRecords /count) : 1;
			}
			else {
				this.estimatedNumRecords = -1;
			}
		}
		
		
		// Estimate output size
		if (this.estimatedOutputSize == -1 && this.estimatedNumRecords != -1 && hints.getAvgBytesPerRecord() >= 1.0f) {
			this.estimatedOutputSize = (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) >= 1 ? 
				(long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) : 1;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// no children, so nothing to compute
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		// because there are no inputs, there are no unclosed branches.
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeAlternativePlans()
	 */
	@Override
	public List<OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		GlobalProperties gp = new GlobalProperties();
		LocalProperties lp = new LocalProperties();

		// first, compute the properties of the output
//		if (getOutputContract() == OutputContract.UniqueKey) {
//			gp.setKeyUnique(true);
//			gp.setPartitioning(PartitionProperty.ANY);
//
//			lp.setKeyUnique(true);
//			lp.setKeysGrouped(true);
//		}

		DataSourceNode candidate = new DataSourceNode(this, gp, lp);

		// compute the costs
		candidate.setCosts(new Costs(0, this.estimatedOutputSize));

		// since there is only a single plan for the data-source, return a list with that element only
		List<OptimizerNode> plans = new ArrayList<OptimizerNode>(1);
		plans.add(candidate);

		if (isBranching()) {
			this.cachedPlans = plans;
		}

		return plans;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}

	
	public boolean isFieldKept(int input, int fieldNumber) {
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readCopyProjectionAnnotations()
	 */
	@Override
	protected void readCopyProjectionAnnotations() {
		// DO NOTHING		
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readReadsAnnotation() {
		// DO NOTHING
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#deriveOutputSchema()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void deriveOutputSchema() {
		
		this.outputSchema = this.computeOutputSchema(Collections.EMPTY_LIST);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputSchema(java.util.List)
	 */
	@Override
	public FieldSet computeOutputSchema(List<FieldSet> inputSchemas) {
		
		if(inputSchemas.size() > 0)
			throw new IllegalArgumentException("DataSourceNode do not have input nodes");
		
		// get the input format class
		@SuppressWarnings("unchecked")
		Class<InputFormat<?, ?>> clazz = (Class<InputFormat<?, ?>>)((GenericDataSource<? extends InputFormat<?, ?>>)getPactContract()).getFormatClass();
		
		InputFormat<?, ?> inputFormat;
		try {
			inputFormat = clazz.newInstance();
			
			if(inputFormat instanceof OutputSchemaProvider) {
				
				inputFormat.configure(getPactContract().getParameters());
				return new FieldSet(((OutputSchemaProvider) inputFormat).getOutputSchema());
			} else {
				return null;
			}
			
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public FieldSet getReadSet(int input) {
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int)
	 */
	@Override
	public FieldSet getWriteSet(int input) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int, java.util.List)
	 */
	@Override
	public FieldSet getWriteSet(int input, List<FieldSet> inputSchemas) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isValidInputSchema(int, int[])
	 */
	@Override
	public boolean isValidInputSchema(int input, FieldSet inputSchema) {
		return false;
	}
}
