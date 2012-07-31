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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.SerializableArrayList;

public class ConnectionInfoLookupResponse implements IOReadableWritable {

	private enum ReturnCode {
		NOT_FOUND, FOUND_AND_RECEIVER_READY, FOUND_BUT_RECEIVER_NOT_READY, JOB_IS_ABORTING
	};

	// was request successful?
	private ReturnCode returnCode;

	/**
	 * Contains next-hop instances, this instance must forward multicast transmissions to.
	 */
	private final SerializableArrayList<RemoteReceiver> remoteTargets = new SerializableArrayList<RemoteReceiver>();

	/**
	 * Contains local ChannelIDs, multicast packets must be forwarded to.
	 */
	private final SerializableArrayList<ChannelID> localTargets = new SerializableArrayList<ChannelID>();

	public ConnectionInfoLookupResponse() {
		this.returnCode = ReturnCode.NOT_FOUND;
	}

	public void addRemoteTarget(final RemoteReceiver remote) {
		this.remoteTargets.add(remote);
	}

	public void addLocalTarget(ChannelID local) {
		this.localTargets.add(local);
	}

	private void setReturnCode(ReturnCode code) {
		this.returnCode = code;
	}

	public List<RemoteReceiver> getRemoteTargets() {
		return this.remoteTargets;
	}

	public List<ChannelID> getLocalTargets() {
		return this.localTargets;
	}

	@Override
	public void read(DataInput in) throws IOException {

		this.localTargets.read(in);
		this.remoteTargets.read(in);

		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		this.localTargets.write(out);
		this.remoteTargets.write(out);

		EnumUtils.writeEnum(out, this.returnCode);

	}

	public boolean receiverNotFound() {

		return (this.returnCode == ReturnCode.NOT_FOUND);
	}

	public boolean receiverNotReady() {

		return (this.returnCode == ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);
	}

	public boolean receiverReady() {

		return (this.returnCode == ReturnCode.FOUND_AND_RECEIVER_READY);
	}

	public boolean isJobAborting() {

		return (this.returnCode == ReturnCode.JOB_IS_ABORTING);
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(final ChannelID targetChannelID) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addLocalTarget(targetChannelID);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(final RemoteReceiver remoteReceiver) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addRemoteTarget(remoteReceiver);

		return response;
	}

	/**
	 * Constructor used to generate a plain ConnectionInfoLookupResponse object to be filled with multicast targets.
	 * 
	 * @return
	 */
	public static ConnectionInfoLookupResponse createReceiverFoundAndReady() {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverNotFound() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.NOT_FOUND);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverNotReady() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);

		return response;
	}

	public static ConnectionInfoLookupResponse createJobIsAborting() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.JOB_IS_ABORTING);

		return response;
	}

	@Override
	public String toString() {
		StringBuilder returnstring = new StringBuilder();
		returnstring.append("local targets (total: " + this.localTargets.size() + "):\n");
		for (ChannelID i : this.localTargets) {
			returnstring.append(i + "\n");
		}
		returnstring.append("remote targets: (total: " + this.remoteTargets.size() + "):\n");
		for (final RemoteReceiver rr : this.remoteTargets) {
			returnstring.append(rr + "\n");
		}
		return returnstring.toString();
	}
}
