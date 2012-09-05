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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.MapStub;


/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see MapStub
 * 
 * @author Aljoscha Krettek
 */
public class MapContract extends SingleInputContract<MapStub>
{	
	private static String DEFAULT_NAME = "<Unnamed Mapper>";
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link MapStub} implementation.
	 * 
	 * @param udf The {@link MapStub} implementation for this Map contract.
	 */
	public static Builder builder(Class<? extends MapStub> udf) {
		return new Builder(udf);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	private MapContract(Builder builder) {
		super(builder.udf, builder.name);
		setInputs(builder.inputs);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 * 
	 * @author Aljoscha Krettek
	 */
	public static class Builder {
		
		/* The required parameters */
		private final Class<? extends MapStub> udf;
		
		/* The optional parameters */
		private List<Contract> inputs;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link MapStub} implementation.
		 * 
		 * @param udf The {@link MapStub} implementation for this Map contract.
		 */
		private Builder(Class<? extends MapStub> udf) {
			this.udf = udf;
			this.inputs = new ArrayList<Contract>();
		}
		
		/**
		 * Sets one or several inputs (union).
		 * 
		 * @param input
		 */
		public Builder input(Contract ...inputs) {
			this.inputs.clear();
			for (Contract c : inputs) {
				this.inputs.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the inputs.
		 * 
		 * @param input
		 */
		public Builder inputs(List<Contract> inputs) {
			this.inputs = inputs;
			return this;
		}
		
		/**
		 * Sets the name of this contract.
		 * 
		 * @param name
		 */
		public Builder name(String name) {
			this.name = name;
			return this;
		}
		
		/**
		 * Creates and returns a MapContract from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public MapContract build() {
			return new MapContract(this);
		}
	}
}
