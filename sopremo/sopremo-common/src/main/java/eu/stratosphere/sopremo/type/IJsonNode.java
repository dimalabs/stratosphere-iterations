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
import java.io.Serializable;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Interface for all JsonNodes.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public interface IJsonNode extends Serializable, Value, Key {
	/**
	 * This enumeration contains all possible types of JsonNode.
	 * 
	 * @author Michael Hopstock
	 * @author Tommy Neubert
	 */
	public enum Type {
		IntNode(IntNode.class, true),
		LongNode(LongNode.class, true),
		BigIntegerNode(BigIntegerNode.class, true),
		DecimalNode(DecimalNode.class, true),
		DoubleNode(DoubleNode.class, true),

		ArrayNode(ArrayNode.class, false),
		ObjectNode(ObjectNode.class, false),
		TextNode(TextNode.class, false),
		BooleanNode(BooleanNode.class, false),
		NullNode(NullNode.class, false),
		MissingNode(MissingNode.class, false),
		CustomNode(AbstractJsonNode.class, false);

		private final Class<? extends AbstractJsonNode> clazz;

		private final boolean numeric;

		private Type(final Class<? extends AbstractJsonNode> clazz, final boolean isNumeric) {
			this.clazz = clazz;
			this.numeric = isNumeric;
		}

		/**
		 * Returns either the node represented by a specific enumeration element is numeric or not.
		 */
		public boolean isNumeric() {
			return this.numeric;
		}

		/**
		 * Returns an instantiable class of the node which is represented by a specific enumeration element.
		 * 
		 * @return the class of the represented node
		 */
		public Class<? extends AbstractJsonNode> getClazz() {
			return this.clazz;
		}

	};

	public abstract void clear();

	/**
	 * Returns the {@link eu.stratosphere.sopremo.type.JsonNode.Type} of this node.
	 * 
	 * @return nodetype
	 */
	public abstract AbstractJsonNode.Type getType();

	/**
	 * Transforms this node into his standard representation.
	 * 
	 * @return standard representation
	 */
	public abstract IJsonNode canonicalize();

	/**
	 * Copies the state of the given node to this node.
	 * 
	 * @param otherNode
	 *        the node of which the state should be copied
	 */
	public abstract void copyValueFrom(IJsonNode otherNode);

	/**
	 * Deserializes this node from a DataInput.
	 * 
	 * @param {@link DataInput} in
	 * @exception {@link IOException}
	 */
	@Override
	public abstract void read(DataInput in) throws IOException;

	/**
	 * Serializes this node into a DataOutput.
	 * 
	 * @param {@link DataOutput} out
	 * @exception {@link IOException}
	 */
	@Override
	public abstract void write(DataOutput out) throws IOException;

	/**
	 * Returns either this node is a representation for a null-value or not.
	 */
	public abstract boolean isNull();

	/**
	 * Returns either this node is a representation for a missing value or not.
	 */
	public abstract boolean isMissing();

	/**
	 * Returns either this node is a representation of a Json-Object or not.
	 */
	public abstract boolean isObject();

	/**
	 * Returns either this node is a representation of a Json-Array or not.
	 */
	public abstract boolean isArray();

	/**
	 * Returns either this node is a representation of a Text-value or not.
	 */
	public abstract boolean isTextual();

	/**
	 * Returns the internal representation of this nodes value.
	 * 
	 * @return this nodes value
	 */
	public abstract Object getJavaValue();

	/**
	 * Compares this node with another.
	 * 
	 * @param other
	 *        the node this node should be compared with
	 * @return result of the comparison
	 */
	@Override
	public abstract int compareTo(final Key other);

	/**
	 * Compares this node with another {@link eu.stratosphere.sopremo.type.IJsonNode}.
	 * 
	 * @param other
	 *        the node this node should be compared with
	 * @return result of the comparison
	 */
	public abstract int compareToSameType(IJsonNode other);

	/**
	 * Appends this nodes string representation to a given {@link StringBuilder}.
	 * 
	 * @param sb
	 *        the StringBuilder
	 * @return a StringBuilder where this nodes string representation is appended
	 */
	public abstract StringBuilder toString(StringBuilder sb);

	public int getMaxNormalizedKeyLen();

	public void copyNormalizedKey(final byte[] target, final int offset, final int len);

}