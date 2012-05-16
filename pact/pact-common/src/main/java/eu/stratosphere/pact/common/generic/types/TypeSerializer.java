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

package eu.stratosphere.pact.common.generic.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;

/**
 * This interface describes the methods that are required for a data type to be handled by the pact
 * runtime. Specifically, this interface contains the serialization and copying methods.
 * <p>
 * The methods in this class are assumed to be stateless, such that it is effectively thread safe. Stateful
 * implementations of the methods may lead to unpredictable side effects and will compromise both stability and
 * correctness of the program.
 *
 * @author Stephan Ewen
 * 
 * @param T The data type that the serializer serializes.
 */
public interface TypeSerializer<T>
{
	/**
	 * Creates a new instance of the data type.
	 * 
	 * @return A new instance of the data type.
	 */
	public T createInstance();
	
	/**
	 * Creates a copy from the given element.
	 * 
	 * @param from The element to copy.
	 * @return A copy of the given element.
	 */
	public T createCopy(T from);
	
	/**
	 * Creates a copy from the given element, storing the copied result in the given target element.
	 * 
	 * @param from The element to be copied.
	 * @param to The target element.
	 */
	public void copyTo(T from, T to);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 * 
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	public int getLength();
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Serializes the given record to the given target output view.
	 * 
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 * @return The number of bytes consumed during the writing.
	 * 
	 * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
	 *                     output view, which may have an underlying I/O channel to which it delegates.
	 */
	public long serialize(T record, DataOutputView target) throws IOException;

	/**
	 * De-serializes a record from the given source input view into the given target record instance.
	 * 
	 * @param record The record instance into which to de-serialize the data.
	 * @param target The input view from which to read the data.
	 * 
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public void deserialize(T target, DataInputView source) throws IOException;
	
	/**
	 * Copies exactly one record from the source input view to the target output view. Whether this operation
	 * works on binary data or partially de-serializes the record to determine its length (such as for records
	 * of variable length) is up to the implementer. Binary copies are typically faster. A copy of a record containing
	 * two integer numbers (8 bytes total) is most efficiently implemented as
	 * {@code target.write(source, 8);}.
	 *  
	 * @param source The input view from which to read the record.
	 * @param target The target output view to which to write the record.
	 * 
	 * @throws IOException Thrown if any of the two views raises an exception.
	 */
	public void copy(DataInputView source, DataOutputView target) throws IOException;
}
