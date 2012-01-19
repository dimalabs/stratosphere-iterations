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

package eu.stratosphere.pact.common.stubs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSet.ConstantSetMode;


/**
 * This class defines the PACT annotations, realized as java annotations. To use
 * a PACT annotation, simply add the annotation above the class declaration of
 * the class that realized the user function. For example, to declare the <i>ReadSet</i> 
 * annotation for a map-type function that realizes a simple filter,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@ReadSet(fields={2})
 * public class MyMapper extends MapStub
 * {
 *     public void map(PactRecord record, Collector out)
 *     {
 *        String s = record.getField(2, PactString.class).toString();
 *        
 *        if (s.contains("...some text...") {
 *            out.collect(record);
 *        }Reduce
 *     }
 * }
 * </blockquote></pre>
 * 
 * Be aware that some annotations should only be used for stubs with as single input 
 * ({@link MapStub}, {@link ReduceStub}) and some only for stubs with two inputs 
 * ({@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class StubAnnotation
{

	/**
	 * Specifies the read set for a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
	 * The {@link ReadSet#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.
	 * 
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 * 
	 * If no ReadSet is specified, all fields are considered to be read.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadSet
	{
		int[] fields();
	}
	
	/**
	 * Specifies the read set for the first input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ReadSetFirst#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads from the first input's tuples and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.   
	 *
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 *
	 * If no ReadSet is specified, all fields are considered to be read.
	 * 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadSetFirst
	{
		int[] fields();
	}
	
	/**
	 * Specifies the read set for the second input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ReadSetSecond#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads from the second input's tuples and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.   
	 *
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 * 
	 * If no ReadSet is specified, all fields are considered to be read.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadSetSecond
	{
		int[] fields();
	}

	/**
	 * Specifies the update set for a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
	 * The {@link ConstantSet#setMode()} attribute specifies whether the {@link ConstantSet#fields()} attribute 
	 * lists fields that are updated {@link ConstantSet.ConstantSetMode#Update} 
	 * or constant (not updated) {@link ConstantSet.ConstantSetMode#Constant}. 
	 * 
	 * A field with index <i>i</i> is considered to be constant if its value is also present in the output record
	 * at the same field index <i>i</i>. 
	 * In any other case, e.g., if the value of a field was changed or the value was moved to another field within
	 * the output record, the field must be considered to be updated.
	 * 
	 * It is very important the the ConstantSet contains at least all fields that are changed in order to guarantee 
	 * correct execution of PACT programs. If the set mode is {@link ConstantSet.ConstantSetMode#Constant}, only 
	 * those fields might be added to the field attribute which are definitely not changed!
	 * 
	 * If you do not specify an ConstantSet, all fields are considered to be updated.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantSet
	{
		public static enum ConstantSetMode {Update, Constant};	
		
		ConstantSetMode setMode() default ConstantSetMode.Constant;
		int[] fields();
	}
	
	/**
	 * Specifies the update set for the first input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ConstantSetFirst#setMode()} attribute specifies whether the {@link ConstantSetFirst#fields()} 
	 * attribute lists fields of the first input that are updated {@link ConstantSet.ConstantSetMode#Update} 
	 * or not modified {@link ConstantSet.ConstantSetMode#Constant}. 
	 * 
	 * A field with index <i>i</i> is considered to be constant if its value is also present in the output record
	 * at the same field index <i>i</i>. 
	 * In any other case, e.g., if the value of a field was changed or the value was moved to another field within
	 * the output record, the field must be considered to be updated.
	 * 
	 * It is very important the the ConstantSet contains at least all fields that are changed in order to guarantee 
	 * correct execution of PACT programs. If the set mode is {@link ConstantSet.ConstantSetMode#Constant}, only 
	 * those fields might be added to the field attribute which are definitely not changed!
	 *
	 * If you do not specify an ConstantSet, all fields are considered to be updated.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantSetFirst
	{
		ConstantSetMode setMode() default ConstantSetMode.Constant;
		int[] fields();
	}
	
	/**
	 * Specifies the update set for the second input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ConstantSetSecond#setMode()} attribute specifies whether the {@link ConstantSetSecond#fields()} 
	 * attribute lists fields of the second input that are updated {@link ConstantSet.ConstantSetMode#Update} 
	 * or not modified {@link ConstantSet.ConstantSetMode#Constant}. 
	 * 
	 * A field with index <i>i</i> is considered to be constant if its value is also present in the output record
	 * at the same field index <i>i</i>. 
	 * In any other case, e.g., if the value of a field was changed or the value was moved to another field within
	 * the output record, the field must be considered to be updated.
	 * 
	 * It is very important the the ConstantSet contains at least all fields that are changed in order to guarantee 
	 * correct execution of PACT programs. If the set mode is {@link ConstantSet.ConstantSetMode#Constant}, only 
	 * those fields might be added to the field attribute which are definitely not changed!
	 *
	 * If you do not specify an ConstantSet, all fields are considered to be updated.
	 * 
	 */	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantSetSecond
	{
		ConstantSetMode setMode() default ConstantSetMode.Constant;
		int[] fields();
	}
	
	/**
	 * Specifies the add set for all kinds of stubs.
	 * The {@link AddSet#fields()} attribute specifies all fields that are added by the stub.
	 * 
	 * A field is added if it is written to a field of the output record that was not used in any
	 * of the stubs inputs.
	 * 
	 * It is very important the the AddSet contains at least all fields that have been added in order to guarantee 
	 * correct execution of PACT programs.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AddSet
	{
		int[] fields();
	}
	
	
	/**
	 * The OutputCardBounds annotation specifies lower and upper bounds on the number of 
	 * records emitted by the stub.
	 * A constants are provided for specical values such as unbounded, cardinality of input, and 
	 * cardinality of first and second input.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface OutCardBounds
	{
		public static final int UNKNOWN = -1;
		public static final int UNBOUNDED = -2;
		public static final int INPUTCARD = -3;
		public static final int FIRSTINPUTCARD = -4;
		public static final int SECONDINPUTCARD = -5;
		
		int lowerBound();
		int upperBound();
	}
	
	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private StubAnnotation() {
	}
}