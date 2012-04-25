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

package eu.stratosphere.nephele.example.speedtest;

import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * This class implements a forwarder task for the speed test. The forwarder task immediately outputs every record it
 * reads.
 * 
 * @author warneke
 */
public final class SpeedTestForwarder extends AbstractTask {

	/**
	 * The record reader used to read incoming records.
	 */
	private MutableRecordReader<SpeedTestRecord> input;

	/**
	 * The record writer used to forward incoming records.
	 */
	private RecordWriter<SpeedTestRecord> output;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.input = new MutableRecordReader<SpeedTestRecord>(this);
		this.output = new RecordWriter<SpeedTestRecord>(this, SpeedTestRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		final SpeedTestRecord record = new SpeedTestRecord();
		while (this.input.next(record)) {
			this.output.emit(record);
		}
	}
}
