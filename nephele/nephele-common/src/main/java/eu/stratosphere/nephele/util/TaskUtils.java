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

package eu.stratosphere.nephele.util;

import eu.stratosphere.nephele.annotations.Stateless;
import eu.stratosphere.nephele.template.AbstractInvokable;

/**
 * This class implements several convenience methods to determine properties of Nephele task classes.
 * 
 * @author warneke
 */
public class TaskUtils {

	/**
	 * Private constructor, so class cannot be instantiated.
	 */
	private TaskUtils() {
	}

	/**
	 * Checks if a task is declared to be stateless.
	 * 
	 * @param taskClass
	 *        the class of the task to check
	 * @return <code>true</code> if the given class is declared to be stateless, <code>false</code> otherwise
	 */
	public static boolean isStateless(final Class<? extends AbstractInvokable> taskClass) {

		return taskClass.isAnnotationPresent(Stateless.class);
	}
}
