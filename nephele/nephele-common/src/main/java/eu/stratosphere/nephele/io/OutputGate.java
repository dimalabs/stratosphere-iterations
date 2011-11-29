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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.ClassUtils;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * In Nephele output gates are a specialization of general gates and connect
 * record writers and output channels. As channels, output gates are always
 * parameterized to a specific type of record which they can transport.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class OutputGate<T extends Record> extends AbstractGate<T> {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(OutputGate.class);

	/**
	 * The list of output channels attached to this gate.
	 */
	private final ArrayList<AbstractOutputChannel<T>> outputChannels = new ArrayList<AbstractOutputChannel<T>>();

	/**
	 * Channel selector to determine which channel is supposed receive the next record.
	 */
	private final ChannelSelector<T> channelSelector;

	/**
	 * The class of the record transported through this output gate.
	 */
	private final Class<T> type;
	
	/**
	 * The listener objects registered for this output gate.
	 */
	private OutputGateListener[] outputGateListeners = null;

	/**
	 * The thread which executes the task connected to the output gate.
	 */
	private Thread executingThread = null;

	/**
	 * Stores whether all records passed to this output gate shall be transmitted through all connected output channels.
	 */
	private final boolean isBroadcast;

	/**
	 * Constructs a new output gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param inputClass
	 *        the class of the record that can be transported through this
	 *        gate
	 * @param index
	 *        the index assigned to this output gate at the {@link Environment} object
	 * @param channelSelector
	 *        the channel selector to be used for this output gate
	 * @param isBroadcast
	 *        <code>true</code> if every records passed to this output gate shall be transmitted through all connected
	 *        output channels, <code>false</code> otherwise
	 */
	public OutputGate(final JobID jobID, final GateID gateID, final Class<T> inputClass, final int index,
			final ChannelSelector<T> channelSelector, final boolean isBroadcast) {

		super(jobID, gateID, index);

		this.isBroadcast = isBroadcast;
		this.type = inputClass;
		
		if (this.isBroadcast) {
			this.channelSelector = null;
		} else {
			if (channelSelector == null) {
				this.channelSelector = new DefaultChannelSelector<T>();
			} else {
				this.channelSelector = channelSelector;
			}
		}
	}

	/**
	 * Returns the type of record that can be transported through this gate.
	 * 
	 * @return the type of record that can be transported through this gate
	 */
	public final Class<T> getType() {
		return this.type;
	}
	
	/**
	 * Adds a new output channel to the output gate.
	 * 
	 * @param outputChannel
	 *        the output channel to be added.
	 */
	private void addOutputChannel(AbstractOutputChannel<T> outputChannel) {
		if (!this.outputChannels.contains(outputChannel)) {
			this.outputChannels.add(outputChannel);
		}
	}

	/**
	 * Removes the output channel with the given ID from the output gate if it
	 * exists.
	 * 
	 * @param outputChannelID
	 *        the ID of the channel to be removed
	 */
	public void removeOutputChannel(ChannelID outputChannelID) {

		for (int i = 0; i < this.outputChannels.size(); i++) {

			final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(i);
			if (outputChannel.getID().equals(outputChannelID)) {
				this.outputChannels.remove(i);
				return;
			}
		}

		LOG.debug("Cannot find output channel with ID " + outputChannelID + " to remove");
	}

	/**
	 * Removes all output channels from the output gate.
	 */
	public void removeAllOutputChannels() {

		this.outputChannels.clear();
	}

	public AbstractOutputChannel<T> replaceChannel(ChannelID oldChannelID, ChannelType newChannelType,
			boolean followsPushModel) {

		AbstractOutputChannel<T> oldOutputChannel = null;

		for (int i = 0; i < this.outputChannels.size(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(i);
			if (outputChannel.getID().equals(oldChannelID)) {
				oldOutputChannel = outputChannel;
				break;
			}
		}

		if (oldOutputChannel == null) {
			return null;
		}

		AbstractOutputChannel<T> newOutputChannel = null;

		switch (newChannelType) {
		case FILE:
			newOutputChannel = new FileOutputChannel<T>(this, oldOutputChannel.getChannelIndex(), oldOutputChannel
				.getID(), oldOutputChannel.getCompressionLevel());
			break;
		case INMEMORY:
			newOutputChannel = new InMemoryOutputChannel<T>(this, oldOutputChannel.getChannelIndex(), oldOutputChannel
				.getID(), oldOutputChannel.getCompressionLevel());
			break;
		case NETWORK:
			newOutputChannel = new NetworkOutputChannel<T>(this, oldOutputChannel.getChannelIndex(), oldOutputChannel
				.getID(), oldOutputChannel.getCompressionLevel());
			break;
		default:
			return null;
		}

		newOutputChannel.setConnectedChannelID(oldOutputChannel.getConnectedChannelID());

		this.outputChannels.set(newOutputChannel.getChannelIndex(), newOutputChannel);

		return newOutputChannel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return false;
	}

	/**
	 * Returns the number of output channels associated with this output gate.
	 * 
	 * @return the number of output channels associated with this output gate
	 */
	public int getNumberOfOutputChannels() {

		return this.outputChannels.size();
	}

	/**
	 * Returns the output channel from position <code>pos</code> of the gate's
	 * internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such
	 *         position does not exist.
	 */
	public AbstractOutputChannel<T> getOutputChannel(int pos) {

		if (pos < this.outputChannels.size())
			return this.outputChannels.get(pos);
		else
			return null;
	}

	/**
	 * Creates a new network output channel and assigns it to the output gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new network output channel
	 */
	public NetworkOutputChannel<T> createNetworkOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final NetworkOutputChannel<T> enoc = new NetworkOutputChannel<T>(this, this.outputChannels.size(), channelID,
			compressionLevel);
		addOutputChannel(enoc);

		return enoc;
	}

	/**
	 * Creates a new file output channel and assigns it to the output gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new file output channel
	 */
	public FileOutputChannel<T> createFileOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final FileOutputChannel<T> efoc = new FileOutputChannel<T>(this, this.outputChannels.size(), channelID,
			compressionLevel);
		addOutputChannel(efoc);

		return efoc;
	}

	/**
	 * Creates a new in-memory output channel and assigns it to the output gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new in-memory output channel
	 */
	public InMemoryOutputChannel<T> createInMemoryOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final InMemoryOutputChannel<T> einoc = new InMemoryOutputChannel<T>(this, this.outputChannels.size(),
			channelID, compressionLevel);
		addOutputChannel(einoc);

		return einoc;
	}

	/**
	 * Requests the output gate to closed. This means the application will send
	 * no records through this gate anymore.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void requestClose() throws IOException, InterruptedException {
		// Close all output channels
		for (int i = 0; i < this.getNumberOfOutputChannels(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.getOutputChannel(i);
			outputChannel.requestClose();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {
		
		boolean allClosed = true;
		
		for (int i = 0; i < this.getNumberOfOutputChannels(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.getOutputChannel(i);
			if (!outputChannel.isClosed()) {
				allClosed = false;
			}
		}

		return allClosed;
	}

	/**
	 * Writes a record to one of the associated output channels. Currently, the
	 * channels are chosen in a simple round-robin fashion. This operation may
	 * block until the respective channel has received the data.
	 * 
	 * @param record
	 *        the record to be written
	 * @throws IOException
	 *         thrown if any error occurs during channel I/O
	 */
	public void writeRecord(T record) throws IOException, InterruptedException {

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		if (this.isBroadcast) {

			if (getChannelType() == ChannelType.INMEMORY) {

				final int numberOfOutputChannels = this.outputChannels.size();
				for (int i = 0; i < numberOfOutputChannels; ++i) {
					this.outputChannels.get(i).writeRecord(record);
				}

			} else {

				// Use optimization for byte buffered channels
				this.outputChannels.get(0).writeRecord(record);
			}

		} else {

			// Non-broadcast gate, use channel selector to select output channels
			final int numberOfOutputChannels = this.outputChannels.size();
			final int[] selectedOutputChannels = this.channelSelector.selectChannels(record, numberOfOutputChannels);

			if (selectedOutputChannels == null) {
				return;
			}

			for (int i = 0; i < selectedOutputChannels.length; ++i) {

				if (selectedOutputChannels[i] < numberOfOutputChannels) {
					final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(selectedOutputChannels[i]);
					outputChannel.writeRecord(record);
				}
			}
		}
	}

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	public void read(DataInput in) throws IOException {

		super.read(in);

		final int numOutputChannels = in.readInt();

		final Class<?>[] parameters = { this.getClass(), int.class, ChannelID.class, CompressionLevel.class };

		for (int i = 0; i < numOutputChannels; i++) {

			final ChannelID channelID = new ChannelID();
			channelID.read(in);
			final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);
			final String className = StringRecord.readString(in);
			Class<? extends IOReadableWritable> c = null;
			try {
				c = ClassUtils.getRecordByName(className);
			} catch (ClassNotFoundException e) {
				LOG.error(e);
			}

			if (c == null) {
				throw new IOException("Class is null!");
			}

			AbstractOutputChannel<T> eoc = null;
			try {
				final Constructor<AbstractOutputChannel<T>> constructor = (Constructor<AbstractOutputChannel<T>>) c
					.getDeclaredConstructor(parameters);

				if (constructor == null) {
					throw new IOException("Constructor is null!");
				}

				constructor.setAccessible(true);

				eoc = constructor.newInstance(this, i, channelID, compressionLevel);
				
			} catch (InstantiationException e) {
				LOG.error(e);
			} catch (IllegalArgumentException e) {
				LOG.error(e);
			} catch (IllegalAccessException e) {
				LOG.error(e);
			} catch (InvocationTargetException e) {
				LOG.error(e);
			} catch (SecurityException e) {
				LOG.error(e);
			} catch (NoSuchMethodException e) {
				LOG.error(e);
			}

			if (eoc == null) {
				throw new IOException("Created output channel is null!");
			}

			eoc.read(in);
			addOutputChannel(eoc);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void write(DataOutput out) throws IOException {

		super.write(out);

		// Output channels
		out.writeInt(this.getNumberOfOutputChannels());

		for (int i = 0; i < getNumberOfOutputChannels(); i++) {
			getOutputChannel(i).getID().write(out);
			EnumUtils.writeEnum(out, getOutputChannel(i).getCompressionLevel());
			StringRecord.writeString(out, getOutputChannel(i).getClass().getName());
			getOutputChannel(i).write(out);
		}

	}

	/**
	 * Returns all the OutputChannels connected to this gate
	 * 
	 * @return the list of OutputChannels connected to this RecordWriter
	 */
	public ArrayList<AbstractOutputChannel<T>> getOutputChannels() {
		return this.outputChannels;
	}

	/**
	 * Registers a new listener object for this output gate.
	 * 
	 * @param outputGateListener
	 *        the listener object to register
	 */
	public void registerOutputGateListener(OutputGateListener outputGateListener) {

		if (this.outputGateListeners == null) {
			this.outputGateListeners = new OutputGateListener[1];
			this.outputGateListeners[0] = outputGateListener;
		} else {
			OutputGateListener[] tmp = this.outputGateListeners;
			this.outputGateListeners = new OutputGateListener[tmp.length + 1];
			System.arraycopy(tmp, 0, this.outputGateListeners, 0, tmp.length);
			this.outputGateListeners[tmp.length] = outputGateListener;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Output " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * Flushes all connected output channels.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while flushing an output channel
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the data to be flushed
	 */
	public void flush() throws IOException, InterruptedException {
		// Flush all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().flush();
		}
	}

	/**
	 * This method is called by one of the attached output channel when its
	 * capacity is currently exhausted and no more data can be written to the channel.
	 * 
	 * @param channelIndex
	 *        the index of the exhausted output channel.
	 */
	public void channelCapacityExhausted(int channelIndex) {

		// notify the listener objects
		if (this.outputGateListeners != null) {
			for (int i = 0; i < this.outputGateListeners.length; ++i) {
				this.outputGateListeners[i].channelCapacityExhausted(channelIndex);
			}
		}
	}

	/**
	 * Checks if this output gate operates in broadcast mode, i.e. all records passed to it are transferred through all
	 * connected output channels.
	 * 
	 * @return <code>true</code> if this output gate operates in broadcast mode, <code>false</code> otherwise
	 */
	public boolean isBroadcast() {

		return this.isBroadcast;
	}

	/**
	 * Returns the output gate's channel selector.
	 * 
	 * @return the output gate's channel selector or <code>null</code> if the gate operates in broadcast mode
	 */
	public ChannelSelector<T> getChannelSelector() {

		return this.channelSelector;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();

		while (it.hasNext()) {
			it.next().releaseResources();
		}
	}
}
