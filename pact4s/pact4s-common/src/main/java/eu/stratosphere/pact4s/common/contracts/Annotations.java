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
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;

@SuppressWarnings("all")
public class Annotations {

	public static final int CARD_UNKNOWN = StubAnnotation.OutCardBounds.UNKNOWN;

	public static final int CARD_UNBOUNDED = StubAnnotation.OutCardBounds.UNBOUNDED;

	public static final int CARD_INPUTCARD = StubAnnotation.OutCardBounds.INPUTCARD;

	public static final int CARD_FIRSTINPUTCARD = StubAnnotation.OutCardBounds.FIRSTINPUTCARD;

	public static final int CARD_SECONDINPUTCARD = StubAnnotation.OutCardBounds.SECONDINPUTCARD;

	public static Annotation getImplicitOperation(ImplicitOperationMode implicitOperation) {
		return new ImplicitOperation(implicitOperation);
	}

	public static Annotation getImplicitOperationFirst(ImplicitOperationMode implicitOperation) {
		return new ImplicitOperationFirst(implicitOperation);
	}

	public static Annotation getImplicitOperationSecond(ImplicitOperationMode implicitOperation) {
		return new ImplicitOperationSecond(implicitOperation);
	}

	public static Annotation getReads(int[] fields) {
		return new Reads(fields);
	}

	public static Annotation getReadsFirst(int[] fields) {
		return new ReadsFirst(fields);
	}

	public static Annotation getReadsSecond(int[] fields) {
		return new ReadsSecond(fields);
	}

	public static Annotation getExplicitCopies(int[] fields) {
		return new ExplicitCopies(fields);
	}

	public static Annotation getExplicitCopiesFirst(int[] fields) {
		return new ExplicitCopiesFirst(fields);
	}

	public static Annotation getExplicitCopiesSecond(int[] fields) {
		return new ExplicitCopiesSecond(fields);
	}

	public static Annotation getExplicitProjections(int[] fields) {
		return new ExplicitProjections(fields);
	}

	public static Annotation getExplicitProjectionsFirst(int[] fields) {
		return new ExplicitProjectionsFirst(fields);
	}

	public static Annotation getExplicitProjectionsSecond(int[] fields) {
		return new ExplicitProjectionsSecond(fields);
	}

	public static Annotation getExplicitModifications(int[] fields) {
		return new ExplicitModifications(fields);
	}

	public static Annotation getOutCardBounds(int lowerBound, int upperBound) {
		return new OutCardBounds(lowerBound, upperBound);
	}

	public static Annotation getCombinable() {
		return new Combinable();
	}

	private static abstract class ImplicitOperations<T extends Annotation> implements Annotation {

		private final Class<T> clazz;

		private final ImplicitOperationMode implicitOperation;

		public ImplicitOperations(Class<T> clazz, ImplicitOperationMode implicitOperation) {
			this.clazz = clazz;
			this.implicitOperation = implicitOperation;
		}

		public ImplicitOperationMode implicitOperation() {
			return implicitOperation;
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

			ImplicitOperationMode otherOperation = getOtherOperation((T) obj);
			return implicitOperation.equals(otherOperation);
		}

		protected abstract ImplicitOperationMode getOtherOperation(T other);

		@Override
		public int hashCode() {
			return (127 * "implicitOperation".hashCode()) ^ implicitOperation.hashCode();
		}
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

	private static class ImplicitOperation extends ImplicitOperations<StubAnnotation.ImplicitOperation> implements
			StubAnnotation.ImplicitOperation {

		public ImplicitOperation(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperation.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperation other) {
			return other.implicitOperation();
		}
	}

	private static class ImplicitOperationFirst extends ImplicitOperations<StubAnnotation.ImplicitOperationFirst>
			implements StubAnnotation.ImplicitOperationFirst {

		public ImplicitOperationFirst(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperationFirst.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperationFirst other) {
			return other.implicitOperation();
		}
	}

	private static class ImplicitOperationSecond extends ImplicitOperations<StubAnnotation.ImplicitOperationSecond>
			implements StubAnnotation.ImplicitOperationSecond {

		public ImplicitOperationSecond(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperationSecond.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperationSecond other) {
			return other.implicitOperation();
		}
	}

	private static class Reads extends Fields<StubAnnotation.Reads> implements StubAnnotation.Reads {

		public Reads(int[] fields) {
			super(StubAnnotation.Reads.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.Reads other) {
			return other.fields();
		}
	}

	private static class ReadsFirst extends Fields<StubAnnotation.ReadsFirst> implements StubAnnotation.ReadsFirst {

		public ReadsFirst(int[] fields) {
			super(StubAnnotation.ReadsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ReadsFirst other) {
			return other.fields();
		}
	}

	private static class ReadsSecond extends Fields<StubAnnotation.ReadsSecond> implements StubAnnotation.ReadsSecond {

		public ReadsSecond(int[] fields) {
			super(StubAnnotation.ReadsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ReadsSecond other) {
			return other.fields();
		}
	}

	private static class ExplicitCopies extends Fields<StubAnnotation.ExplicitCopies> implements
			StubAnnotation.ExplicitCopies {

		public ExplicitCopies(int[] fields) {
			super(StubAnnotation.ExplicitCopies.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopies other) {
			return other.fields();
		}
	}

	private static class ExplicitCopiesFirst extends Fields<StubAnnotation.ExplicitCopiesFirst> implements
			StubAnnotation.ExplicitCopiesFirst {

		public ExplicitCopiesFirst(int[] fields) {
			super(StubAnnotation.ExplicitCopiesFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopiesFirst other) {
			return other.fields();
		}
	}

	private static class ExplicitCopiesSecond extends Fields<StubAnnotation.ExplicitCopiesSecond> implements
			StubAnnotation.ExplicitCopiesSecond {

		public ExplicitCopiesSecond(int[] fields) {
			super(StubAnnotation.ExplicitCopiesSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopiesSecond other) {
			return other.fields();
		}
	}

	private static class ExplicitProjections extends Fields<StubAnnotation.ExplicitProjections> implements
			StubAnnotation.ExplicitProjections {

		public ExplicitProjections(int[] fields) {
			super(StubAnnotation.ExplicitProjections.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjections other) {
			return other.fields();
		}
	}

	private static class ExplicitProjectionsFirst extends Fields<StubAnnotation.ExplicitProjectionsFirst> implements
			StubAnnotation.ExplicitProjectionsFirst {

		public ExplicitProjectionsFirst(int[] fields) {
			super(StubAnnotation.ExplicitProjectionsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjectionsFirst other) {
			return other.fields();
		}
	}

	private static class ExplicitProjectionsSecond extends Fields<StubAnnotation.ExplicitProjectionsSecond> implements
			StubAnnotation.ExplicitProjectionsSecond {

		public ExplicitProjectionsSecond(int[] fields) {
			super(StubAnnotation.ExplicitProjectionsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjectionsSecond other) {
			return other.fields();
		}
	}

	private static class ExplicitModifications extends Fields<StubAnnotation.ExplicitModifications> implements
			StubAnnotation.ExplicitModifications {

		public ExplicitModifications(int[] fields) {
			super(StubAnnotation.ExplicitModifications.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitModifications other) {
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
