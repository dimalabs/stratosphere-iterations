package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.AbstractPactTask;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public abstract class AbstractMinimalTask extends AbstractTask {

	protected MutableObjectIterator<PactRecord>[] inputs;
	protected TaskConfig config;
	protected OutputCollector output;
	protected ClassLoader classLoader;
	
	@Override
	public final void registerInputOutput() {
		this.config = new TaskConfig(getRuntimeConfiguration());
		try {
			this.classLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
		} catch (IOException ioe) {
			throw new RuntimeException("The ClassLoader for the user code could not be instantiated from the library cache.", ioe);
		}
		
		initInputs();
		initOutputs();
		initInternal();
		initTask();
	}
	
	protected void initInternal() {
		//Default implementation does nothing
	}
	
	protected abstract void initTask();
	
	/**
	 * Creates the record readers for the number of inputs as defined by {@link #getNumberOfInputs()}.
	 */
	protected void initInputs()
	{
		int numInputs = getNumberOfInputs();
		
		@SuppressWarnings("unchecked")
		final MutableObjectIterator<PactRecord>[] inputs = new MutableObjectIterator[numInputs];
		
		for (int i = 0; i < numInputs; i++)
		{
			final ShipStrategy shipStrategy = config.getInputShipStrategy(i);
			DistributionPattern dp = null;
			
			switch (shipStrategy)
			{
			case FORWARD:
			case PARTITION_LOCAL_HASH:
			case PARTITION_LOCAL_RANGE:
				dp = new PointwiseDistributionPattern();
				break;
			case PARTITION_HASH:
			case PARTITION_RANGE:
			case BROADCAST:
			case SFR:
				dp = new BipartiteDistributionPattern();
				break;
			default:
				throw new RuntimeException("Invalid input ship strategy provided for input " + 
					i + ": " + shipStrategy.name());
			}
			
			inputs[i] = new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, dp));
		}
		
		this.inputs = inputs;
	}
	
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategies for each writer.
	 */
	protected void initOutputs()
	{
		this.output = AbstractPactTask.getOutputCollector(this, this.config, getClass().getClassLoader(), this.config.getNumOutputs());
	}
	
	protected void waitForPreviousTask(MutableObjectIterator<PactRecord> input) throws IOException {
		PactRecord tmp = new PactRecord();
		while(input.next(tmp)) {
			
		}
	}
	
	protected Class<? extends Key>[] loadKeyClasses() {
		try {
			return config.getLocalStrategyKeyClasses(classLoader);
		} catch (Exception ex) {
			throw new RuntimeException("Error loading key classes", ex);
		}
	}
	
	protected <T> T initStub(Class<T> stubSuperClass) {
		try {
			@SuppressWarnings("unchecked")
			Class<T> stubClass = (Class<T>) this.config.getStubClass(stubSuperClass, classLoader);
			
			return InstantiationUtil.instantiate(stubClass, stubSuperClass);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("The stub implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex); 
		}
	}
	
	/**
	 * Gets the number of inputs (= Nephele Gates and Readers) that the task has.
	 * 
	 * @return The number of inputs.
	 */
	public abstract int getNumberOfInputs();

}
