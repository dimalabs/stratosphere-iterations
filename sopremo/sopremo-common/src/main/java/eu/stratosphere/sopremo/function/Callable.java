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
package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ISerializableSopremoType;

/**
 * @author Arvid Heise
 */
public abstract class Callable<Result, InputType> extends AbstractSopremoType implements ISerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7623937906556576557L;

	public abstract Result call(InputType params, Result target, EvaluationContext context);
}
