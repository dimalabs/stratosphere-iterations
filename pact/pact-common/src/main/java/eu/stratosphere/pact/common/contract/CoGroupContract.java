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

package eu.stratosphere.pact.common.contract;

import java.util.List;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;


/**
 * CrossContract represents a Match InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * CoGroup works on two inputs and calls the first-order user function of a {@link CoGroupStub} 
 * with the groups of records sharing the same key (one group per input) independently.
 * 
 * @see CoGroupStub
 */
public class CoGroupContract extends DualInputContract<CoGroupStub>
{	
	private static String DEFAULT_NAME = "<Unnamed CoGrouper>";		// the default name for contracts
	
	/**
	 * The ordering for the order inside a group from input one.
	 */
	private Ordering secondaryOrder1;
	
	/**
	 * The ordering for the order inside a group from input two.
	 */
	private Ordering secondaryOrder2;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation
	 * and a default name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn) {
		this(c, keyType, firstKeyColumn, secondKeyColumn, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param name The name of PACT.
	 */
	@SuppressWarnings("unchecked")
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn, String name) {
		this(c, new Class[] {keyType}, new int[] {firstKeyColumn}, new int[] {secondKeyColumn}, name);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[]) {
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], String name) {
		super(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
											Contract input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			Contract input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the order of the elements within a group for the given input.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @param order The order for the elements in a group.
	 */
	public void setSecondaryOrder(int inputNum, Ordering order) {
		if (inputNum == 0)
			this.secondaryOrder1 = order;
		else if (inputNum == 1)
			this.secondaryOrder2 = order;
		else
			throw new IndexOutOfBoundsException();
	}
	
	/**
	 * Sets the order of the elements within a group for the first input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setSecondaryOrderForInputOne(Ordering order) {
		setSecondaryOrder(0, order);
	}
	
	/**
	 * Sets the order of the elements within a group for the second input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setSecondaryOrderForInputTwo(Ordering order) {
		setSecondaryOrder(1, order);
	}
	
	/**
	 * Gets the secondary order for an input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @return The secondary order.
	 */
	public Ordering getSecondaryOrder(int inputNum) {
		if (inputNum == 0)
			return this.secondaryOrder1;
		else if (inputNum == 1)
			return this.secondaryOrder2;
		else
			throw new IndexOutOfBoundsException();
	}
	
	/**
	 * Gets the secondary order for the first input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The secondary order for the first input.
	 */
	public Ordering getSecondaryOrderForInputOne() {
		return getSecondaryOrder(0);
	}
	
	/**
	 * Gets the secondary order for the second input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The secondary order for the second input.
	 */
	public Ordering getSecondaryOrderForInputTwo() {
		return getSecondaryOrder(1);
	}
}
