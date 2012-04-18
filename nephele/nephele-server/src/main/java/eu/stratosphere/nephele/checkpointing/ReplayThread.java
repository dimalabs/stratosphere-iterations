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

package eu.stratosphere.nephele.checkpointing;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileChannelWrapper;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;

final class ReplayThread extends Thread {

	private static final String REPLAY_SUFFIX = " (Replay)";

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * The buffer size in bytes to use for the meta data file channel.
	 */
	private static final int BUFFER_SIZE = 4096;

	private final ExecutionVertexID vertexID;

	private final ExecutionObserver executionObserver;

	private final boolean isCheckpointLocal;

	private final boolean isCheckpointComplete;

	private final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap;

	private final AtomicBoolean restartRequested = new AtomicBoolean(false);

	ReplayThread(final ExecutionVertexID vertexID, final ExecutionObserver executionObserver, final String taskName,
			final boolean isCheckpointLocal, final boolean isCheckpointComplete,
			final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap) {
		super((taskName == null ? "Unkown" : taskName) + REPLAY_SUFFIX);

		this.vertexID = vertexID;
		this.executionObserver = executionObserver;
		this.isCheckpointLocal = isCheckpointLocal;
		this.isCheckpointComplete = isCheckpointComplete;
		this.outputBrokerMap = outputBrokerMap;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Now the actual program starts to run
		changeExecutionState(ExecutionState.REPLAYING, null);

		// If the task has been canceled in the mean time, do not even start it
		if (this.executionObserver.isCanceled()) {
			changeExecutionState(ExecutionState.CANCELED, null);
			return;
		}

		try {

			// Replay the actual checkpoint
			replayCheckpoint();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

		} catch (Exception e) {

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Task finished running, but there may be some unconsumed data in the brokers
		changeExecutionState(ExecutionState.FINISHING, null);

		try {
			// Wait until all output broker have sent all of their data
			waitForAllOutputBrokerToFinish();
		} catch (Exception e) {

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	private void resetAllOutputBroker() {

		final Iterator<ReplayOutputChannelBroker> it = this.outputBrokerMap.values().iterator();
		while (it.hasNext()) {
			it.next().reset();
		}

	}

	private void waitForAllOutputBrokerToFinish() throws IOException, InterruptedException {

		while (!this.executionObserver.isCanceled()) {
			boolean finished = true;
			final Iterator<ReplayOutputChannelBroker> it = this.outputBrokerMap.values().iterator();
			while (it.hasNext()) {

				if (!it.next().hasFinished()) {
					finished = false;
				}
			}

			if (finished) {
				break;
			}

			Thread.sleep(SLEEPINTERVAL);
		}
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	void restart() {

		changeExecutionState(ExecutionState.STARTING, null);
		this.restartRequested.set(true);
		// Fake transition to replaying here to prevent deadlocks
		changeExecutionState(ExecutionState.REPLAYING, null);
	}

	private void replayCheckpoint() throws Exception {

		final CheckpointDeserializer deserializer = new CheckpointDeserializer(this.vertexID, !this.isCheckpointLocal);

		final Path checkpointPath = this.isCheckpointLocal ? CheckpointUtils.getLocalCheckpointPath() : CheckpointUtils
			.getDistributedCheckpointPath();

		if (checkpointPath == null) {
			throw new IOException("Cannot determine checkpoint path for vertex " + this.vertexID);
		}

		// The file system the checkpoint's meta data is stored on
		final FileSystem fileSystem = checkpointPath.getFileSystem();

		int metaDataIndex = 0;

		Buffer firstDeserializedFileBuffer = null;
		FileChannel fileChannel = null;

		try {

			while (true) {

				if (this.restartRequested.compareAndSet(true, false)) {
					metaDataIndex = 0;
					// Reset all the output broker in case we here restarted
					resetAllOutputBroker();
				}

				// Try to locate the meta data file
				final Path metaDataFile = checkpointPath.suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX
					+ "_" + this.vertexID + "_" + metaDataIndex);

				while (!fileSystem.exists(metaDataFile)) {

					// Try to locate the final meta data file
					final Path finalMetaDataFile = checkpointPath.suffix(Path.SEPARATOR
						+ CheckpointUtils.METADATA_PREFIX
						+ "_" + this.vertexID + "_final");

					if (fileSystem.exists(finalMetaDataFile)) {
						return;
					}

					if (this.isCheckpointComplete) {
						throw new FileNotFoundException("Cannot find meta data file " + metaDataIndex
							+ " for checkpoint of vertex " + this.vertexID);
					}

					// Wait for the file to be created
					Thread.sleep(1000);

					if (this.executionObserver.isCanceled()) {
						return;
					}
				}

				fileChannel = getFileChannel(fileSystem, metaDataFile);

				while (true) {
					try {
						deserializer.read(fileChannel);

						final TransferEnvelope transferEnvelope = deserializer.getFullyDeserializedTransferEnvelope();
						if (transferEnvelope != null) {

							final ReplayOutputChannelBroker broker = this.outputBrokerMap.get(transferEnvelope
								.getSource());
							if (broker == null) {
								throw new IOException("Cannot find output broker for channel "
										+ transferEnvelope.getSource());
							}

							final Buffer srcBuffer = transferEnvelope.getBuffer();
							if (srcBuffer != null) {

								// Prevent underlying file from being closed
								if (firstDeserializedFileBuffer == null) {
									firstDeserializedFileBuffer = srcBuffer.duplicate();
								}

								if (transferEnvelope.getSequenceNumber() < broker.getNextEnvelopeToSend()) {
									srcBuffer.recycleBuffer();
									continue;
								}

								final Buffer destBuffer = broker.requestEmptyBufferBlocking(srcBuffer.size());
								srcBuffer.copyToBuffer(destBuffer);
								transferEnvelope.setBuffer(destBuffer);
								srcBuffer.recycleBuffer();
							}

							broker.outputEnvelope(transferEnvelope);

							if (this.executionObserver.isCanceled()) {
								return;
							}
						}
					} catch (EOFException eof) {
						// Close the file channel
						fileChannel.close();
						fileChannel = null;
						// Increase the index of the meta data file
						++metaDataIndex;
						break;
					}
				}
			}

		} finally {
			if (firstDeserializedFileBuffer != null) {
				firstDeserializedFileBuffer.recycleBuffer();
				firstDeserializedFileBuffer = null;
			}
			if (fileChannel != null) {
				fileChannel.close();
				fileChannel = null;
			}
		}
	}

	private FileChannel getFileChannel(final FileSystem fs, final Path p) throws IOException {

		// Bypass FileSystem API for local checkpoints
		if (this.isCheckpointLocal) {

			final URI uri = p.toUri();
			return new FileInputStream(uri.getPath()).getChannel();
		}

		return new FileChannelWrapper(fs, p, BUFFER_SIZE, (short) -1);
	}
}
