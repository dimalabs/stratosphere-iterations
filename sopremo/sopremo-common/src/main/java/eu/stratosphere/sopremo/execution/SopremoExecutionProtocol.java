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
package eu.stratosphere.sopremo.execution;

import eu.stratosphere.nephele.protocols.VersionedProtocol;

/**
 * A general purpose interface for everything that executes meteor scripts synchronically and asynchronically.
 * 
 * @author Arvid Heise
 */
public interface SopremoExecutionProtocol extends VersionedProtocol, LibraryTransferProtocol {
	/**
	 * Executes the query specified in the {@link ExecutionRequest}.
	 * 
	 * @param request
	 *        the request with the query
	 * @return the {@link ExecutionResponse}
	 */
	public ExecutionResponse execute(ExecutionRequest request);

	/**
	 * Queries the state of the given job.
	 * 
	 * @param jobId
	 *        the job id
	 * @return the {@link ExecutionResponse} with the state
	 */
	public ExecutionResponse getState(SopremoID jobId);
}
