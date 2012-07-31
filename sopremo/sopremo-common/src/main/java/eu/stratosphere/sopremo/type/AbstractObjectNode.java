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
package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Arvid Heise
 */
public abstract class AbstractObjectNode extends AbstractJsonNode implements IObjectNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4669433750960349150L;

	/**
	 * Initializes AbstractObjectNode.
	 */
	public AbstractObjectNode() {
		super();
	}

	@Override
	public final Type getType() {
		return Type.ObjectNode;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++)
			this.put(in.readUTF(), SopremoUtil.deserializeNode(in));
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.size());

		for (final Entry<String, IJsonNode> entry : this) {
			out.writeUTF(entry.getKey());
			SopremoUtil.serializeNode(out, entry.getValue());
		}

	}

	@Override
	public final boolean isObject() {
		return true;
	}

	@Override
	public void copyValueFrom(IJsonNode otherNode) {
		this.clear();
		for (Entry<String, IJsonNode> child : this)
			this.put(child.getKey(), child.getValue());
	}

}