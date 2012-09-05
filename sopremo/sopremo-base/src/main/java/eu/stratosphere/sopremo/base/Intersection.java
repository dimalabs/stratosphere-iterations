package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;

/**
 * Calculates the set-based intersection of two or more input streams.<br>
 * A value <i>v</i> is emitted only iff <i>v</i> is contained at least once in every input stream.<br>
 * A value is emitted at most once.
 * 
 * @author Arvid Heise
 */
@Name(verb = "intersect")
public class Intersection extends SetOperation<Intersection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2204883087931986053L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.base.SetOperation#createBinaryOperations(eu.stratosphere.sopremo.JsonStream,
	 * eu.stratosphere.sopremo.JsonStream)
	 */
	@Override
	protected ElementaryOperator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput) {
		return new TwoInputIntersection().withInputs(leftInput, rightInput);
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputIntersection extends ElementaryOperator<TwoInputIntersection> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7207641826328647442L;

		public static class Implementation extends SopremoCoGroup {
			@Override
			protected void coGroup(final IArrayNode values1, final IArrayNode values2,
					final JsonCollector out) {
				if (!values1.isEmpty() && !values2.isEmpty())
					out.collect(values1.get(0));
			}
		}
	}
}
