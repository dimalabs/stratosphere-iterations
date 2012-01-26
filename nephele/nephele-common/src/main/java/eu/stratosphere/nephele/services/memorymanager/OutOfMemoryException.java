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

package eu.stratosphere.nephele.services.memorymanager;


import eu.stratosphere.nephele.services.ServiceException;


/**
 * An exception to be thrown when a request to allocate memory cannot be served due to a lack of available memory.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class OutOfMemoryException extends ServiceException
{
	private static final long serialVersionUID = 7948265635897479726L;
	
	private final int segmentsRequested;
	private final int segmentsAvailable;
	
	/**
	 * Creates an exception without error message. The requested number of segments and the number of available
	 * segments is unknown (set to <code>-1</code>).
	 */
	public OutOfMemoryException() {
		super();
		
		this.segmentsRequested = -1;
		this.segmentsAvailable = -1;
	}
	
	/**
	 * Creates an exception describing the given requested number of segments and the number of
	 * available segments.
	 * 
	 * @param segmentsRequested The requested number of segments.
	 * @param segmentsAvailable The available number of segments.
	 */
	public OutOfMemoryException(int segmentsRequested, int segmentsAvailable) {
		super("Could not provide the requested " + segmentsRequested + " segments of memory. Only " + 
			segmentsAvailable + " segments available.");
		this.segmentsRequested = segmentsRequested;
		this.segmentsAvailable = segmentsAvailable;
	}

	/**
	 * Gets the number of requested segments.
	 * 
	 * @return The number of requested segments.
	 */
	public int getSegmentsRequested() {
		return segmentsRequested;
	}

	/**
	 * Gets the number of available segments.
	 * 
	 * @return The number of available segments.
	 */
	public int getSegmentsAvailable() {
		return segmentsAvailable;
	}
}
