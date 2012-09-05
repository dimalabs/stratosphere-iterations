package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Merges several arrays by taking the first non-null value for each respective array.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.ARRAY, transitive = true, minNodes = 1, maxNodes = OptimizerHints.UNBOUND, iterating = true)
public class ArrayMerger extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6884623565349727369L;

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		final ArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);

		for (final IJsonNode nextNode : (IArrayNode) node)
			if (nextNode != NullNode.getInstance()) {
				final IArrayNode array = (IArrayNode) nextNode;
				for (int index = 0; index < array.size(); index++)
					if (targetArray.size() <= index)
						targetArray.add(array.get(index));
					else if (this.isNull(targetArray.get(index)) && !this.isNull(array.get(index)))
						targetArray.set(index, array.get(index));
			}

		return targetArray;
	}

	private boolean isNull(final IJsonNode value) {
		return value == null || value.isNull();
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("[*]+...+[*]");
	}

}
