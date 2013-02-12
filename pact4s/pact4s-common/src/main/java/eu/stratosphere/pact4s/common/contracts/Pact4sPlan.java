/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact4s.common.contracts;

import java.util.Collection;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;

/**
 *
 */
public class Pact4sPlan extends Plan {

	public Pact4sPlan(Collection<GenericDataSink> sinks, String jobName) {
		super(sinks, jobName);
	}

	public Pact4sPlan(Collection<GenericDataSink> sinks) {
		super(sinks);
	}

	public Pact4sPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	public Pact4sPlan(GenericDataSink sink) {
		super(sink);
	}

	@Override
	public String getPostPassClassName() {
		return "eu.stratosphere.pact4s.common.Pact4sPostPass";
	}
}
