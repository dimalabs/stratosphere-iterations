package eu.stratosphere.pact4s.common.contracts;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;

@SuppressWarnings("all")
public class Annotations {

	public static abstract class ImplicitOperations<T extends Annotation> implements Annotation {

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

	public static abstract class Fields<T extends Annotation> implements Annotation {

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

	public static class ImplicitOperation extends ImplicitOperations<StubAnnotation.ImplicitOperation> implements
			StubAnnotation.ImplicitOperation {

		public ImplicitOperation(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperation.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperation other) {
			return other.implicitOperation();
		}
	}

	public static class ImplicitOperationFirst extends ImplicitOperations<StubAnnotation.ImplicitOperationFirst>
			implements StubAnnotation.ImplicitOperationFirst {

		public ImplicitOperationFirst(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperationFirst.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperationFirst other) {
			return other.implicitOperation();
		}
	}

	public static class ImplicitOperationSecond extends ImplicitOperations<StubAnnotation.ImplicitOperationSecond>
			implements StubAnnotation.ImplicitOperationSecond {

		public ImplicitOperationSecond(ImplicitOperationMode implicitOperation) {
			super(StubAnnotation.ImplicitOperationSecond.class, implicitOperation);
		}

		@Override
		protected ImplicitOperationMode getOtherOperation(StubAnnotation.ImplicitOperationSecond other) {
			return other.implicitOperation();
		}
	}

	public static class Reads extends Fields<StubAnnotation.Reads> implements StubAnnotation.Reads {

		public Reads(int[] fields) {
			super(StubAnnotation.Reads.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.Reads other) {
			return other.fields();
		}
	}

	public static class ReadsFirst extends Fields<StubAnnotation.ReadsFirst> implements StubAnnotation.ReadsFirst {

		public ReadsFirst(int[] fields) {
			super(StubAnnotation.ReadsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ReadsFirst other) {
			return other.fields();
		}
	}

	public static class ReadsSecond extends Fields<StubAnnotation.ReadsSecond> implements StubAnnotation.ReadsSecond {

		public ReadsSecond(int[] fields) {
			super(StubAnnotation.ReadsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ReadsSecond other) {
			return other.fields();
		}
	}

	public static class ExplicitCopies extends Fields<StubAnnotation.ExplicitCopies> implements
			StubAnnotation.ExplicitCopies {

		public ExplicitCopies(int[] fields) {
			super(StubAnnotation.ExplicitCopies.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopies other) {
			return other.fields();
		}
	}

	public static class ExplicitCopiesFirst extends Fields<StubAnnotation.ExplicitCopiesFirst> implements
			StubAnnotation.ExplicitCopiesFirst {

		public ExplicitCopiesFirst(int[] fields) {
			super(StubAnnotation.ExplicitCopiesFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopiesFirst other) {
			return other.fields();
		}
	}

	public static class ExplicitCopiesSecond extends Fields<StubAnnotation.ExplicitCopiesSecond> implements
			StubAnnotation.ExplicitCopiesSecond {

		public ExplicitCopiesSecond(int[] fields) {
			super(StubAnnotation.ExplicitCopiesSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitCopiesSecond other) {
			return other.fields();
		}
	}

	public static class ExplicitProjections extends Fields<StubAnnotation.ExplicitProjections> implements
			StubAnnotation.ExplicitProjections {

		public ExplicitProjections(int[] fields) {
			super(StubAnnotation.ExplicitProjections.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjections other) {
			return other.fields();
		}
	}

	public static class ExplicitProjectionsFirst extends Fields<StubAnnotation.ExplicitProjectionsFirst> implements
			StubAnnotation.ExplicitProjectionsFirst {

		public ExplicitProjectionsFirst(int[] fields) {
			super(StubAnnotation.ExplicitProjectionsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjectionsFirst other) {
			return other.fields();
		}
	}

	public static class ExplicitProjectionsSecond extends Fields<StubAnnotation.ExplicitProjectionsSecond> implements
			StubAnnotation.ExplicitProjectionsSecond {

		public ExplicitProjectionsSecond(int[] fields) {
			super(StubAnnotation.ExplicitProjectionsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitProjectionsSecond other) {
			return other.fields();
		}
	}

	public static class ExplicitModifications extends Fields<StubAnnotation.ExplicitModifications> implements
			StubAnnotation.ExplicitModifications {

		public ExplicitModifications(int[] fields) {
			super(StubAnnotation.ExplicitModifications.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ExplicitModifications other) {
			return other.fields();
		}
	}

	public static class OutCardBounds implements Annotation, StubAnnotation.OutCardBounds {

		public static final int UNKNOWN = StubAnnotation.OutCardBounds.UNKNOWN;

		public static final int UNBOUNDED = StubAnnotation.OutCardBounds.UNBOUNDED;

		public static final int INPUTCARD = StubAnnotation.OutCardBounds.INPUTCARD;

		public static final int FIRSTINPUTCARD = StubAnnotation.OutCardBounds.FIRSTINPUTCARD;

		public static final int SECONDINPUTCARD = StubAnnotation.OutCardBounds.SECONDINPUTCARD;

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

	public static class Combinable implements Annotation, ReduceContract.Combinable {

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
