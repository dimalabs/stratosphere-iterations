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

package eu.stratosphere.pact4s.common.contracts;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stubs.StubAnnotation;

@SuppressWarnings("all")
public class Annotations {

	public static final int CARD_UNKNOWN = StubAnnotation.OutCardBounds.UNKNOWN;

	public static final int CARD_UNBOUNDED = StubAnnotation.OutCardBounds.UNBOUNDED;

	public static final int CARD_INPUTCARD = StubAnnotation.OutCardBounds.INPUTCARD;

	public static final int CARD_FIRSTINPUTCARD = StubAnnotation.OutCardBounds.FIRSTINPUTCARD;

	public static final int CARD_SECONDINPUTCARD = StubAnnotation.OutCardBounds.SECONDINPUTCARD;

	public static Annotation getConstantFields(int[] fields) {
		return new ConstantFields(fields);
	}

	public static Annotation getConstantFieldsFirst(int[] fields) {
		return new ConstantFieldsFirst(fields);
	}

	public static Annotation getConstantFieldsSecond(int[] fields) {
		return new ConstantFieldsSecond(fields);
	}

	public static Annotation getConstantFieldsExcept(int[] fields) {
		return new ConstantFieldsExcept(fields);
	}

	public static Annotation getConstantFieldsFirstExcept(int[] fields) {
		return new ConstantFieldsFirstExcept(fields);
	}

	public static Annotation getConstantFieldsSecondExcept(int[] fields) {
		return new ConstantFieldsSecondExcept(fields);
	}

	public static Annotation getOutCardBounds(int lowerBound, int upperBound) {
		return new OutCardBounds(lowerBound, upperBound);
	}

	public static Annotation getCombinable() {
		return new Combinable();
	}

	private static abstract class Fields<T extends Annotation> implements Annotation {

		private final Class<T> clazz;

		private final int[] fields;

		public Fields(Class<T> clazz, int[] fields) {
			this.clazz = clazz;
			this.fields = fields;
		}

		public int[] fields() {
			return fields;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return clazz;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !annotationType().isAssignableFrom(obj.getClass()))
				return false;

			if (!annotationType().equals(((Annotation) obj).annotationType()))
				return false;

			int[] otherFields = getOtherFields((T) obj);
			return Arrays.equals(fields, otherFields);
		}

		protected abstract int[] getOtherFields(T other);

		@Override
		public int hashCode() {
			return (127 * "fields".hashCode()) ^ Arrays.hashCode(fields);
		}
	}

	private static class ConstantFields extends Fields<StubAnnotation.ConstantFields> implements
			StubAnnotation.ConstantFields {

		public ConstantFields(int[] fields) {
			super(StubAnnotation.ConstantFields.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFields other) {
			return other.fields();
		}
	}

	private static class ConstantFieldsFirst extends Fields<StubAnnotation.ConstantFieldsFirst> implements
			StubAnnotation.ConstantFieldsFirst {

		public ConstantFieldsFirst(int[] fields) {
			super(StubAnnotation.ConstantFieldsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsFirst other) {
			return other.fields();
		}
	}

	private static class ConstantFieldsSecond extends Fields<StubAnnotation.ConstantFieldsSecond> implements
			StubAnnotation.ConstantFieldsSecond {

		public ConstantFieldsSecond(int[] fields) {
			super(StubAnnotation.ConstantFieldsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsSecond other) {
			return other.fields();
		}
	}

	private static class ConstantFieldsExcept extends Fields<StubAnnotation.ConstantFieldsExcept> implements
			StubAnnotation.ConstantFieldsExcept {

		public ConstantFieldsExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsExcept other) {
			return other.fields();
		}
	}

	private static class ConstantFieldsFirstExcept extends Fields<StubAnnotation.ConstantFieldsFirstExcept> implements
			StubAnnotation.ConstantFieldsFirstExcept {

		public ConstantFieldsFirstExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsFirstExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsFirstExcept other) {
			return other.fields();
		}
	}

	private static class ConstantFieldsSecondExcept extends Fields<StubAnnotation.ConstantFieldsSecondExcept> implements
			StubAnnotation.ConstantFieldsSecondExcept {

		public ConstantFieldsSecondExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsSecondExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsSecondExcept other) {
			return other.fields();
		}
	}

	private static class OutCardBounds implements Annotation, StubAnnotation.OutCardBounds {

		private final int lowerBound;

		private final int upperBound;

		public OutCardBounds(int lowerBound, int upperBound) {
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		@Override
		public int lowerBound() {
			return lowerBound;
		}

		@Override
		public int upperBound() {
			return upperBound;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return StubAnnotation.OutCardBounds.class;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !annotationType().isAssignableFrom(obj.getClass()))
				return false;

			if (!annotationType().equals(((Annotation) obj).annotationType()))
				return false;

			int otherLower = ((StubAnnotation.OutCardBounds) obj).lowerBound();
			int otherUpper = ((StubAnnotation.OutCardBounds) obj).upperBound();

			return lowerBound == otherLower && upperBound == otherUpper;
		}

		@Override
		public int hashCode() {
			int lb = (127 * "lowerBound".hashCode()) ^ Integer.valueOf(lowerBound).hashCode();
			int ub = (127 * "upperBound".hashCode()) ^ Integer.valueOf(upperBound).hashCode();
			return lb + ub;
		}
	}

	private static class Combinable implements Annotation, ReduceContract.Combinable {

		public Combinable() {
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return ReduceContract.Combinable.class;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !annotationType().isAssignableFrom(obj.getClass()))
				return false;

			if (!annotationType().equals(((Annotation) obj).annotationType()))
				return false;

			return true;
		}

		@Override
		public int hashCode() {
			return 0;
		}
	}
}
