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

package eu.stratosphere.pact.runtime.util;

import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * A LastRepeatableIterator allows to emit the latest emitted object again. 
 * 
 * @author Matthias Ringwald
 * @param <E> The type of element that the iterator iterates over.
 */
public interface LastRepeatableMutableObjectIterator<E> extends MutableObjectIterator<E>
{
	/**
	 * Return the last returned element again.
	 * 
	 * @return The last returned element.
	 */
	public boolean repeatLast(E target);
}
