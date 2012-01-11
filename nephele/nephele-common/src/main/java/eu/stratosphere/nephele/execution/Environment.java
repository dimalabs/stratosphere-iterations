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

package eu.stratosphere.nephele.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The user code of every Nephele task runs inside an <code>Environment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class Environment implements Runnable, IOReadableWritable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(Environment.class);

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * List of output gates created by the task.
	 */
	private final List<OutputGate<? extends Record>> outputGates = new CopyOnWriteArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates created by the task.
	 */
	private final List<InputGate<? extends Record>> inputGates = new CopyOnWriteArrayList<InputGate<? extends Record>>();

	/**
	 * List of output gates which have to be rebound to a task after transferring the environment to a TaskManager.
	 */
	private final List<OutputGate<? extends Record>> unboundOutputGates = new CopyOnWriteArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates which have to be rebound to a task after transferring the environment to a TaskManager.
	 */
	private final List<InputGate<? extends Record>> unboundInputGates = new CopyOnWriteArrayList<InputGate<? extends Record>>();

	/**
	 * The memory manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private volatile MemoryManager memoryManager;

	/**
	 * The io manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private volatile IOManager ioManager;

	/**
	 * Class of the task to run in this environment.
	 */
	private volatile Class<? extends AbstractInvokable> invokableClass = null;

	/**
	 * Instance of the class to be run in this environment.
	 */
	private volatile AbstractInvokable invokable = null;

	/**
	 * The thread executing the task in the environment.
	 */
	private volatile Thread executingThread = null;

	/**
	 * The ID of the job this task belongs to.
	 */
	private volatile JobID jobID = null;

	/**
	 * The runtime configuration of the task encapsulated in the environment object.
	 */
	private volatile Configuration runtimeConfiguration = null;

	/**
	 * The input split provider that can be queried for new input splits.
	 */
	private volatile InputSplitProvider inputSplitProvider = null;

	/**
	 * The observer object for the task's execution.
	 */
	private volatile ExecutionObserver executionObserver = null;

	/**
	 * The current number of subtasks the respective task is split into.
	 */
	private volatile int currentNumberOfSubtasks = 1;

	/**
	 * The index of this subtask in the subtask group.
	 */
	private volatile int indexInSubtaskGroup = 0;

	/**
	 * The name of the task running in this environment.
	 */
	private volatile String taskName;

	/**
	 * Creates a new environment object which contains the runtime information for the encapsulated Nephele task.
	 * 
	 * @param jobID
	 *        the ID of the original Nephele job
	 * @param taskName
	 *        the name of task running in this environment
	 * @param invokableClass
	 *        invokableClass the class that should be instantiated as a Nephele task
	 * @param runtimeConfiguration
	 *        the configuration object which was attached to the original {@link JobVertex}
	 */
	public Environment(final JobID jobID, final String taskName,
			final Class<? extends AbstractInvokable> invokableClass, final Configuration runtimeConfiguration) {
		this.jobID = jobID;
		this.taskName = taskName;
		this.invokableClass = invokableClass;
		this.runtimeConfiguration = runtimeConfiguration;
	}

	/**
	 * Empty constructor used to deserialize the object.
	 */
	public Environment() {
	}

	/**
	 * Returns the invokable object that represents the Nephele task.
	 * 
	 * @return the invokable object that represents the Nephele task
	 */
	public AbstractInvokable getInvokable() {
		return this.invokable;
	}

	/**
	 * Returns the ID of the job from the original job graph. It is used by the library cache manager to find the
	 * required
	 * libraries for executing the assigned Nephele task.
	 * 
	 * @return the ID of the job from the original job graph
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Checks if the environment has unbound input gates.
	 * 
	 * @return <code>true</code> if the environment has unbound input gates, <code>false</code> otherwise
	 */
	public boolean hasUnboundInputGates() {

		return (this.unboundInputGates.size() > 0);
	}

	/**
	 * Checks if the environment has unbound output gates.
	 * 
	 * @return <code>true</code> if the environment has unbound output gates, <code>false</code> otherwise
	 */
	public boolean hasUnboundOutputGates() {

		return (this.unboundOutputGates.size() > 0);
	}

	/**
	 * Retrieves and removes the unbound output gate with the given ID from the list of unbound output gates.
	 * 
	 * @param gateID
	 *        the index of the unbound output gate
	 * @return the unbound output gate with the given ID, or <code>null</code> if no such gate exists
	 */
	public OutputGate<? extends Record> getUnboundOutputGate(final int gateID) {

		if (this.unboundOutputGates.size() == 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No unbound output gates");
			}
			return null;
		}
		return this.unboundOutputGates.remove(gateID);
	}

	/**
	 * Retrieves and removes unbound input gate with the given ID from the list of unbound input gates.
	 * 
	 * @param gateID
	 *        the index of the unbound input gate
	 * @return the unbound input gate with the given ID, or <code>null</code> if no such gate exists
	 */
	public InputGate<? extends Record> getUnboundInputGate(final int gateID) {

		if (this.unboundInputGates.size() == 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No unbound input gates");
			}
			return null;
		}

		return this.unboundInputGates.remove(gateID);
	}

	/**
	 * Creates a new instance of the Nephele task and registers it with its
	 * environment.
	 * 
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public void instantiateInvokable() throws Exception {

		// Test and set, protected by synchronized block
		synchronized (this) {

			if (this.invokableClass == null) {
				LOG.fatal("InvokableClass is null");
				return;
			}

			try {
				this.invokable = this.invokableClass.newInstance();
			} catch (InstantiationException e) {
				LOG.error(e);
			} catch (IllegalAccessException e) {
				LOG.error(e);
			}
		}

		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();

		if (this.jobID == null) {
			LOG.warn("jobVertexID is null");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		if (invokable == null) {
			LOG.fatal("ExecutionEnvironment has no Invokable set");
		}

		// Now the actual program starts to run
		changeExecutionState(ExecutionState.RUNNING, null);

		// If the task has been canceled in the mean time, do not even start it
		if (this.executionObserver.isCanceled()) {
			changeExecutionState(ExecutionState.CANCELED, null);
			return;
		}

		try {

			// Activate input channels
			activateInputChannels();

			this.invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

		} catch (Exception e) {

			if (!this.executionObserver.isCanceled()) {

				// Perform clean up when the task failed and has been not canceled by the user
				try {
					this.invokable.cancel();
				} catch (Exception e2) {
					LOG.error(StringUtils.stringifyException(e2));
				}
			}

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Task finished running, but there may be unconsumed output data in some of the channels
		changeExecutionState(ExecutionState.FINISHING, null);

		try {
			// If there is any unclosed input gate, close it and propagate close operation to corresponding output gate
			closeInputGates();

			// First, close all output gates to indicate no records will be emitted anymore
			requestAllOutputGatesToClose();

			// Wait until all input channels are closed
			waitForInputChannelsToBeClosed();

			// Now we wait until all output channels have written out their data and are closed
			waitForOutputChannelsToBeClosed();
		} catch (Exception e) {

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Release all resources that may currently be allocated by the individual channels
		releaseAllChannelResources();

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	/**
	 * Activates all of the task's input channels.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while transmitting one of the activation requests to the corresponding
	 *         output channels
	 * @throws InterruptedException
	 *         throws if the task is interrupted while waiting for the activation process to complete
	 */
	private void activateInputChannels() throws IOException, InterruptedException {

		for (int i = 0; i < getNumberOfInputGates(); ++i) {
			final InputGate<? extends Record> eig = getInputGate(i);
			for (int j = 0; j < eig.getNumberOfInputChannels(); ++j) {
				eig.getInputChannel(j).activate();
			}
		}
	}

	/**
	 * Registers an output gate with the environment.
	 * 
	 * @param outputGate
	 *        the output gate to be registered with the environment
	 */
	public void registerOutputGate(final OutputGate<? extends Record> outputGate) {
		
		this.outputGates.add(outputGate);
	}

	/**
	 * Registers an input gate with the environment.
	 * 
	 * @param inputGate
	 *        the input gate to be registered with the environment
	 */
	public void registerInputGate(final InputGate<? extends Record> inputGate) {
		
		this.inputGates.add(inputGate);
	}

	/**
	 * Returns the number of output gates registered with this environment.
	 * 
	 * @return the number of output gates registered with this environment
	 */
	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	/**
	 * Returns the number of input gates registered with this environment.
	 * 
	 * @return the number of input gates registered with this environment
	 */
	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	/**
	 * Returns the registered input gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the input gate to return
	 * @return the input gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public InputGate<? extends Record> getInputGate(final int pos) {
		if (pos < this.inputGates.size()) {
			return this.inputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the registered output gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the output gate to return
	 * @return the output gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public OutputGate<? extends Record> getOutputGate(final int pos) {
		if (pos < this.outputGates.size()) {
			return this.outputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the thread which is assigned to execute the user code.
	 * 
	 * @return the thread which is assigned to execute the user code
	 */
	public Thread getExecutingThread() {

		synchronized (this) {

			if (this.executingThread == null) {
				if (this.taskName == null) {
					this.executingThread = new Thread(this);
				} else {
					this.executingThread = new Thread(this, this.taskName);
				}
			}

			return this.executingThread;
		}
	}

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {

		// Read job vertex id
		this.jobID = new JobID();
		this.jobID.read(in);

		// Read the task name
		this.taskName = StringRecord.readString(in);

		// Read names of required jar files
		final String[] requiredJarFiles = new String[in.readInt()];
		for (int i = 0; i < requiredJarFiles.length; i++) {
			requiredJarFiles[i] = StringRecord.readString(in);
		}

		// Now register data with the library manager
		LibraryCacheManager.register(this.jobID, requiredJarFiles);

		// Get ClassLoader from Library Manager
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);

		// Read the name of the invokable class;
		final String invokableClassName = StringRecord.readString(in);

		if (invokableClassName == null) {
			throw new IOException("invokableClassName is null");
		}

		try {
			this.invokableClass = (Class<? extends AbstractInvokable>) Class.forName(invokableClassName, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Class " + invokableClassName + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}

		final int numOuputGates = in.readInt();

		for (int i = 0; i < numOuputGates; i++) {

			final GateID gateID = new GateID();
			gateID.read(in);

			final String typeClassName = StringRecord.readString(in);
			Class<? extends Record> type = null;
			try {
				type = (Class<? extends Record>) Class.forName(typeClassName, true, cl);
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + typeClassName + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			}

			final boolean isBroadcast = in.readBoolean();

			ChannelSelector<? extends Record> channelSelector = null;
			if (!isBroadcast) {

				final String channelSelectorClassName = StringRecord.readString(in);
				try {
					channelSelector = (ChannelSelector<? extends Record>) Class.forName(channelSelectorClassName, true,
						cl).newInstance();
				} catch (InstantiationException e) {
					throw new IOException(StringUtils.stringifyException(e));
				} catch (IllegalAccessException e) {
					throw new IOException(StringUtils.stringifyException(e));
				} catch (ClassNotFoundException e) {
					throw new IOException(StringUtils.stringifyException(e));
				}

				channelSelector.read(in);
			}

			@SuppressWarnings("rawtypes")
			final OutputGate<? extends Record> eog = new OutputGate(this.jobID, gateID, type, i, channelSelector,
				isBroadcast);
			eog.read(in);
			this.outputGates.add(eog);
			// Mark as unbound for reconnection of RecordWriter
			this.unboundOutputGates.add(eog);
		}

		final int numInputGates = in.readInt();

		for (int i = 0; i < numInputGates; i++) {

			final GateID gateID = new GateID();
			gateID.read(in);

			final String deserializerClassName = StringRecord.readString(in);
			RecordDeserializer<? extends Record> recordDeserializer = null;
			Class<? extends RecordDeserializer<? extends Record>> deserializerClass = null;
			try {
				deserializerClass = (Class<? extends RecordDeserializer<? extends Record>>) cl
					.loadClass(deserializerClassName);
				recordDeserializer = deserializerClass.newInstance();

			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (InstantiationException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (IllegalAccessException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			recordDeserializer.setClassLoader(cl);
			recordDeserializer.read(in);

			final String distributionPatternClassName = StringRecord.readString(in);
			DistributionPattern distributionPattern = null;
			Class<? extends DistributionPattern> distributionPatternClass = null;
			try {
				distributionPatternClass = (Class<? extends DistributionPattern>) cl
					.loadClass(distributionPatternClassName);

				distributionPattern = distributionPatternClass.newInstance();

			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (InstantiationException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (IllegalAccessException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			@SuppressWarnings("rawtypes")
			final InputGate<? extends Record> eig = new InputGate(this.jobID, gateID, recordDeserializer, i,
				distributionPattern);
			eig.read(in);
			this.inputGates.add(eig);
			// Mark as unbound for reconnection of RecordReader
			this.unboundInputGates.add(eig);
		}

		// The configuration object
		this.runtimeConfiguration = new Configuration();
		this.runtimeConfiguration.read(in);

		// The current of number subtasks
		this.currentNumberOfSubtasks = in.readInt();
		// The index in the subtask group
		this.indexInSubtaskGroup = in.readInt();

		// Finally, instantiate the invokable object
		try {
			instantiateInvokable();
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write out job vertex id
		if (this.jobID == null) {
			throw new IOException("this.jobID is null");
		}

		this.jobID.write(out);

		// Write the task name
		StringRecord.writeString(out, this.taskName);

		// Write out the names of the required jar files
		final String[] requiredJarFiles = LibraryCacheManager.getRequiredJarFiles(this.jobID);

		out.writeInt(requiredJarFiles.length);
		for (int i = 0; i < requiredJarFiles.length; i++) {
			StringRecord.writeString(out, requiredJarFiles[i]);
		}

		// Write out the name of the invokable class
		if (this.invokableClass == null) {
			throw new IOException("this.invokableClass is null");
		}

		StringRecord.writeString(out, this.invokableClass.getName());

		// Output gates
		out.writeInt(getNumberOfOutputGates());
		for (int i = 0; i < getNumberOfOutputGates(); i++) {
			final OutputGate<? extends Record> outputGate = getOutputGate(i);
			outputGate.getGateID().write(out);
			StringRecord.writeString(out, outputGate.getType().getName());
			out.writeBoolean(outputGate.isBroadcast());
			if (!outputGate.isBroadcast()) {
				// Write out class name of channel selector
				StringRecord.writeString(out, outputGate.getChannelSelector().getClass().getName());
				outputGate.getChannelSelector().write(out);
			}

			getOutputGate(i).write(out);
		}

		// Input gates
		out.writeInt(getNumberOfInputGates());
		for (int i = 0; i < getNumberOfInputGates(); i++) {
			final InputGate<? extends Record> inputGate = getInputGate(i);
			inputGate.getGateID().write(out);
			StringRecord.writeString(out, inputGate.getRecordDeserializer().getClass().getName());
			inputGate.getRecordDeserializer().write(out);
			StringRecord.writeString(out, inputGate.getDistributionPattern().getClass().getName());
			getInputGate(i).write(out);
		}

		// The configuration object
		this.runtimeConfiguration.write(out);

		// The current of number subtasks
		out.writeInt(this.currentNumberOfSubtasks);
		// The index in the subtask group
		out.writeInt(this.indexInSubtaskGroup);
	}

	/**
	 * Blocks until all output channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the output channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForOutputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfOutputGates(); i++) {
				final OutputGate<? extends Record> eog = getOutputGate(i);
				if (!eog.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Blocks until all input channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the input channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForInputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfInputGates(); i++) {
				final InputGate<? extends Record> eig = getInputGate(i);
				if (!eig.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Closes all input gates which are not already closed.
	 */
	private void closeInputGates() throws IOException, InterruptedException {

		for (int i = 0; i < getNumberOfInputGates(); i++) {
			final InputGate<? extends Record> eig = getInputGate(i);
			// Important: close must be called on each input gate exactly once
			eig.close();
		}

	}

	/**
	 * Requests all output gates to be closed.
	 */
	private void requestAllOutputGatesToClose() throws IOException, InterruptedException {
		for (int i = 0; i < getNumberOfOutputGates(); i++) {
			this.getOutputGate(i).requestClose();
		}
	}

	/**
	 * Returns a duplicate (deep copy) of this environment object. However, duplication
	 * does not cover the gates arrays. They must be manually reconstructed.
	 * 
	 * @return a duplicate (deep copy) of this environment object
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public Environment duplicateEnvironment() throws Exception {

		final Environment duplicatedEnvironment = new Environment();
		duplicatedEnvironment.invokableClass = this.invokableClass;
		duplicatedEnvironment.jobID = this.jobID;
		duplicatedEnvironment.taskName = this.taskName;
		Thread tmpThread = null;
		synchronized (this) {
			tmpThread = this.executingThread;
		}
		synchronized (duplicatedEnvironment) {
			duplicatedEnvironment.executingThread = tmpThread;
		}
		duplicatedEnvironment.runtimeConfiguration = this.runtimeConfiguration;

		// We instantiate the invokable of the new environment
		duplicatedEnvironment.instantiateInvokable();

		return duplicatedEnvironment;
	}

	/**
	 * Returns the current {@link IOManager}.
	 * 
	 * @return the current {@link IOManager}.
	 */
	public IOManager getIOManager() {
		return this.ioManager;
	}

	/**
	 * Sets the {@link IOManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link IOManager}
	 */
	public void setIOManager(final IOManager ioManager) {
		this.ioManager = ioManager;
	}

	/**
	 * Returns the current {@link MemoryManager}.
	 * 
	 * @return the current {@link MemoryManager}.
	 */
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	/**
	 * Sets the {@link MemoryManager}.
	 * 
	 * @param memoryManager
	 *        the new {@link MemoryManager}
	 */
	public void setMemoryManager(final MemoryManager memoryManager) {
		this.memoryManager = memoryManager;
	}

	/**
	 * Returns the runtime configuration object which was attached to the original {@link JobVertex}.
	 * 
	 * @return the runtime configuration object which was attached to the original {@link JobVertex}
	 */
	public Configuration getRuntimeConfiguration() {
		return this.runtimeConfiguration;
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 * 
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {

		return this.currentNumberOfSubtasks;
	}

	/**
	 * Sets the current number of subtasks the respective task is split into.
	 * 
	 * @param currentNumberOfSubtasks
	 *        the current number of subtasks the respective task is split into
	 */
	public void setCurrentNumberOfSubtasks(final int currentNumberOfSubtasks) {

		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	public int getIndexInSubtaskGroup() {

		return this.indexInSubtaskGroup;
	}

	/**
	 * Sets the index of this subtask in the subtask group.
	 * 
	 * @param indexInSubtaskGroup
	 *        the index of this subtask in the subtask group
	 */
	public void setIndexInSubtaskGroup(final int indexInSubtaskGroup) {

		this.indexInSubtaskGroup = indexInSubtaskGroup;
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	/**
	 * Returns the name of the task running in this environment.
	 * 
	 * @return the name of the task running in this environment
	 */
	public String getTaskName() {

		return this.taskName;
	}

	/**
	 * Sets the execution observer for this environment.
	 * 
	 * @param executionObserver
	 *        the execution observer for this environment
	 */
	public void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	/**
	 * Sets the input split provider for this environment.
	 * 
	 * @param inputSplitProvider
	 *        the input split provider for this environment
	 */
	public void setInputSplitProvider(final InputSplitProvider inputSplitProvider) {
		this.inputSplitProvider = inputSplitProvider;
	}

	/**
	 * Returns the input split provider assigned to this environment.
	 * 
	 * @return the input split provider or <code>null</code> if no such provider has been assigned to this environment.
	 */
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	/**
	 * Sends a notification that objects that a new user thread has been started to the execution observer.
	 * 
	 * @param userThread
	 *        the user thread which has been started
	 */
	public void userThreadStarted(final Thread userThread) {

		if (this.executionObserver != null) {
			this.executionObserver.userThreadStarted(userThread);
		}
	}

	/**
	 * Sends a notification that a user thread has finished to the execution observer.
	 * 
	 * @param userThread
	 *        the user thread which has finished
	 */
	public void userThreadFinished(final Thread userThread) {

		if (this.executionObserver != null) {
			this.executionObserver.userThreadFinished(userThread);
		}
	}

	/**
	 * Releases the allocated resources (particularly buffer) of input and output channels attached to this task. This
	 * method should only be called after the respected task has stopped running.
	 */
	private void releaseAllChannelResources() {

		for (int i = 0; i < getNumberOfInputGates(); i++) {
			this.getInputGate(i).releaseAllChannelResources();
		}

		for (int i = 0; i < getNumberOfOutputGates(); i++) {
			this.getOutputGate(i).releaseAllChannelResources();
		}
	}
}
