package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Represents a logical OR.
 */
@OptimizerHints(scope = Scope.ANY)
public class OrExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private final BooleanExpression[] expressions;

	/**
	 * Initializes an OrExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param expressions
	 *        the expressions which evaluate to the input for this OrExpression
	 */
	public OrExpression(final BooleanExpression... expressions) {
		this.expressions = new BooleanExpression[expressions.length];
		for (int index = 0; index < expressions.length; index++)
			this.expressions[index] = UnaryExpression.wrap(expressions[index]);
		this.expectedTarget = BooleanNode.class;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final OrExpression other = (OrExpression) obj;
		return Arrays.equals(this.expressions, other.expressions);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		// we can ignore 'target' because no new Object is created
		for (final EvaluationExpression booleanExpression : this.expressions)
			if (booleanExpression.evaluate(node, null, context) == BooleanNode.TRUE)
				return BooleanNode.TRUE;
		return BooleanNode.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		for (int index = 0; index < this.expressions.length; index++)
			this.expressions[index] = (BooleanExpression) this.expressions[index].transformRecursively(function);
		return function.call(this);
	}

	/**
	 * Returns the expressions.
	 * 
	 * @return the expressions
	 */
	public BooleanExpression[] getExpressions() {
		return this.expressions;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = super.hashCode();
		result = prime * result + Arrays.hashCode(this.expressions);
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.expressions[0]);
		for (int index = 1; index < this.expressions.length; index++)
			builder.append(" OR ").append(this.expressions[index]);
	}

	/**
	 * Creates an OrExpression with the given {@link BooleanExpression}.
	 * 
	 * @param expression
	 *        the expression that should be used as the condition
	 * @return the created OrExpression
	 */
	public static OrExpression valueOf(final BooleanExpression expression) {
		if (expression instanceof OrExpression)
			return (OrExpression) expression;
		return new OrExpression(expression);
	}

	/**
	 * Creates an OrExpression with the given {@link BooleanExpression}s.
	 * 
	 * @param childConditions
	 *        the expressions that should be used as conditions for the created OrExpression
	 * @return the created OrExpression
	 */
	public static OrExpression valueOf(final List<? extends EvaluationExpression> childConditions) {
		List<BooleanExpression> booleans = BooleanExpression.ensureBooleanExpressions(childConditions);
		if (booleans.size() == 1)
			return valueOf(booleans.get(0));
		return new OrExpression(booleans.toArray(new BooleanExpression[booleans.size()]));
	}
	
}