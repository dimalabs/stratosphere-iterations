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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;

public interface ByteBufferedInputChannelBroker {

	public void releaseConsumedReadBuffer();

	public BufferPairResponse getReadBufferToConsume();

	/**
	 * Forwards the given event to the connected network output channel on a best effort basis.
	 * 
	 * @param event
	 *        the event to be transferred
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the event to be transfered
	 * @throws IOException
	 *         thrown if an I/O error occurs while transfering the event
	 */
	void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException;
}
