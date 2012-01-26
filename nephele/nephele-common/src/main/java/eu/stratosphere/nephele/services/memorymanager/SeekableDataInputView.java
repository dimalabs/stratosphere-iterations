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


/**
 * Interface marking a {@link DataInputViewV2} as seekable. Seekable views can set the position where they
 * read from.
 * 
 * @author Stephan Ewen
 */
public interface SeekableDataInputView extends DataInputViewV2
{
	/**
	 * Gets the current read position.
	 * 
	 * @return The current read position.
	 */
	public long getReadPosition();
	
	/**
	 * Sets the current read position.
	 * 
	 * @param position The current read position.
	 */
	public void setReadPosition(long position);
}
