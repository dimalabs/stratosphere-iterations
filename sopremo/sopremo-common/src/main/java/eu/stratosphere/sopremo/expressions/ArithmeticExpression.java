package eu.stratosphere.sopremo.expressions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.EnumMap;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.NumberCoercer;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.AbstractJsonNode.Type;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;

/**
 * Represents all basic arithmetic expressions covering the addition, subtraction, division, and multiplication for
 * various types of numbers.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.NUMBER, minNodes = 2, maxNodes = 2, transitive = true)
public class ArithmeticExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9103414139002479181L;

	private final ArithmeticExpression.ArithmeticOperator operator;

	private EvaluationExpression firstOperand, secondOperand;

	private IJsonNode lastFirstValue, lastSecondValue;

	/**
	 * Returns the first operand.
	 * 
	 * @return the first operand
	 */
	public EvaluationExpression getFirstOperand() {
		return this.firstOperand;
	}

	/**
	 * Sets the first operand to the specified value.
	 * 
	 * @param firstOperand
	 *        the operand to set
	 */
	public void setFirstOperand(final EvaluationExpression firstOperand) {
		if (firstOperand == null)
			throw new NullPointerException("firstOperand must not be null");

		this.firstOperand = firstOperand;
	}

	/**
	 * Sets the second operand to the specified value.
	 * 
	 * @param secondOperand
	 *        the operand to set
	 */
	public void setSecondOperand(final EvaluationExpression secondOperand) {
		if (secondOperand == null)
			throw new NullPointerException("second operand must not be null");

		this.secondOperand = secondOperand;
	}

	/**
	 * Returns the second operand.
	 * 
	 * @return the second operand
	 */
	public EvaluationExpression getSecondOperand() {
		return this.secondOperand;
	}

	/**
	 * Returns the operator.
	 * 
	 * @return the operator
	 */
	public ArithmeticExpression.ArithmeticOperator getOperator() {
		return this.operator;
	}

	/**
	 * Initializes Arithmetic with two {@link EvaluationExpression}s and an {@link ArithmeticOperator} in infix
	 * notation.
	 * 
	 * @param op1
	 *        the first operand
	 * @param operator
	 *        the operator
	 * @param op2
	 *        the
	 */
	public ArithmeticExpression(final EvaluationExpression op1, final ArithmeticOperator operator,
			final EvaluationExpression op2) {
		this.operator = operator;
		this.firstOperand = op1;
		this.secondOperand = op2;
		this.expectedTarget = INumericNode.class;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ArithmeticExpression other = (ArithmeticExpression) obj;
		return this.firstOperand.equals(other.firstOperand)
			&& this.operator.equals(other.operator)
			&& this.secondOperand.equals(other.secondOperand);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		// TODO Reuse target (problem: result could be any kind of NumericNode)
		this.lastFirstValue = this.firstOperand.evaluate(node, this.lastFirstValue, context);
		this.lastSecondValue = this.secondOperand.evaluate(node, this.lastSecondValue, context);
		return this.operator.evaluate((INumericNode) this.lastFirstValue, (INumericNode) this.lastSecondValue, target);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		this.firstOperand = this.firstOperand.transformRecursively(function);
		this.secondOperand = this.secondOperand.transformRecursively(function);
		return function.call(this);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 59 * result + this.firstOperand.hashCode();
		result = 59 * result + this.operator.hashCode();
		result = 59 * result + this.secondOperand.hashCode();
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.firstOperand);
		builder.append(' ');
		builder.append(this.operator);
		builder.append(' ');
		builder.append(this.secondOperand);
	}

	/**
	 * Closed set of basic arithmetic operators.
	 * 
	 * @author Arvid Heise
	 */
	public static enum ArithmeticOperator {
		/**
		 * Addition
		 */
		ADDITION("+", new IntegerEvaluator() {
			@Override
			protected int evaluate(final int left, final int right) {
				return left + right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(final long left, final long right) {
				return left + right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(final double left, final double right) {
				return left + right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(final BigInteger left, final BigInteger right) {
				return left.add(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(final BigDecimal left, final BigDecimal right) {
				return left.add(right);
			}
		}),
		/**
		 * Subtraction
		 */
		SUBTRACTION("-", new IntegerEvaluator() {
			@Override
			protected int evaluate(final int left, final int right) {
				return left - right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(final long left, final long right) {
				return left - right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(final double left, final double right) {
				return left - right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(final BigInteger left, final BigInteger right) {
				return left.subtract(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(final BigDecimal left, final BigDecimal right) {
				return left.subtract(right);
			}
		}),
		/**
		 * Multiplication
		 */
		MULTIPLICATION("*", new IntegerEvaluator() {
			@Override
			protected int evaluate(final int left, final int right) {
				return left * right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(final long left, final long right) {
				return left * right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(final double left, final double right) {
				return left * right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(final BigInteger left, final BigInteger right) {
				return left.multiply(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(final BigDecimal left, final BigDecimal right) {
				return left.multiply(right);
			}
		}),
		/**
		 * Division
		 */
		DIVISION("/", DivisionEvaluator.INSTANCE, DivisionEvaluator.INSTANCE, new DoubleEvaluator() {
			@Override
			protected double evaluate(final double left, final double right) {
				return left / right;
			}
		}, DivisionEvaluator.INSTANCE, DivisionEvaluator.INSTANCE);

		private final String sign;

		private final Map<AbstractJsonNode.Type, NumberEvaluator<INumericNode>> typeEvaluators =
			new EnumMap<AbstractJsonNode.Type, NumberEvaluator<INumericNode>>(
				AbstractJsonNode.Type.class);

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private ArithmeticOperator(final String sign, final NumberEvaluator integerEvaluator,
				final NumberEvaluator longEvaluator,
				final NumberEvaluator doubleEvaluator, final NumberEvaluator bigIntegerEvaluator,
				final NumberEvaluator bigDecimalEvaluator) {
			this.sign = sign;
			this.typeEvaluators.put(AbstractJsonNode.Type.IntNode, integerEvaluator);
			this.typeEvaluators.put(AbstractJsonNode.Type.LongNode, longEvaluator);
			this.typeEvaluators.put(AbstractJsonNode.Type.DoubleNode, doubleEvaluator);
			this.typeEvaluators.put(AbstractJsonNode.Type.BigIntegerNode, bigIntegerEvaluator);
			this.typeEvaluators.put(AbstractJsonNode.Type.DecimalNode, bigDecimalEvaluator);
		}

		/**
		 * Performs the binary operation on the two operands after coercing both values to a common number type.
		 * 
		 * @param left
		 *        the left operand
		 * @param right
		 *        the right operand
		 * @return the result of the operation
		 */
		public INumericNode evaluate(final INumericNode left, final INumericNode right, IJsonNode target) {
			final Type widerType = NumberCoercer.INSTANCE.getWiderType(left, right);
			final NumberEvaluator<INumericNode> evaluator = this.typeEvaluators.get(widerType);
			final Class<? extends INumericNode> implementationType = evaluator.getReturnType();
			INumericNode numericTarget = SopremoUtil.ensureType(target, implementationType);
			evaluator.evaluate(left, right, numericTarget);
			return numericTarget;
		}

		@Override
		public String toString() {
			return this.sign;
		}
	}

	private abstract static class BigDecimalEvaluator implements NumberEvaluator<DecimalNode> {
		protected abstract BigDecimal evaluate(BigDecimal left, BigDecimal right);

		@Override
		public void evaluate(INumericNode left, INumericNode right, DecimalNode numericTarget) {
			numericTarget.setValue(this.evaluate(left.getDecimalValue(), right.getDecimalValue()));
		}

		@Override
		public Class<DecimalNode> getReturnType() {
			return DecimalNode.class;
		}
	}

	private abstract static class BigIntegerEvaluator implements NumberEvaluator<BigIntegerNode> {
		protected abstract BigInteger evaluate(BigInteger left, BigInteger right);

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.ArithmeticExpression.NumberEvaluator#evaluate(eu.stratosphere.sopremo
		 * .type.INumericNode, eu.stratosphere.sopremo.type.INumericNode, eu.stratosphere.sopremo.type.NumericNode)
		 */
		@Override
		public void evaluate(INumericNode left, INumericNode right, BigIntegerNode numericTarget) {
			numericTarget.setValue(this.evaluate(left.getBigIntegerValue(), right.getBigIntegerValue()));
		}

		@Override
		public Class<BigIntegerNode> getReturnType() {
			return BigIntegerNode.class;
		}
	}

	/**
	 * Taken from Groovy's org.codehaus.groovy.runtime.typehandling.BigDecimalMath
	 * 
	 * @author Arvid Heise
	 */
	static class DivisionEvaluator implements NumberEvaluator<DecimalNode> {
		private static final DivisionEvaluator INSTANCE = new DivisionEvaluator();

		// This is an arbitrary value, picked as a reasonable choice for a precision
		// for typical user math when a non-terminating result would otherwise occur.
		public static final int DIVISION_EXTRA_PRECISION = 10;

		// This is an arbitrary value, picked as a reasonable choice for a rounding point
		// for typical user math.
		public static final int DIVISION_MIN_SCALE = 10;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.ArithmeticExpression.NumberEvaluator#evaluate(eu.stratosphere.sopremo
		 * .type.INumericNode, eu.stratosphere.sopremo.type.INumericNode, eu.stratosphere.sopremo.type.NumericNode)
		 */
		@Override
		public void evaluate(INumericNode left, INumericNode right, DecimalNode numericTarget) {
			numericTarget.setValue(divideImpl(left.getDecimalValue(), right.getDecimalValue()));
		}

		public static BigDecimal divideImpl(final BigDecimal bigLeft, final BigDecimal bigRight) {
			try {
				return bigLeft.divide(bigRight);
			} catch (final ArithmeticException e) {
				// set a DEFAULT precision if otherwise non-terminating
				final int precision = Math.max(bigLeft.precision(), bigRight.precision()) + DIVISION_EXTRA_PRECISION;
				BigDecimal result = bigLeft.divide(bigRight, new MathContext(precision));
				final int scale = Math.max(Math.max(bigLeft.scale(), bigRight.scale()), DIVISION_MIN_SCALE);
				if (result.scale() > scale)
					result = result.setScale(scale, BigDecimal.ROUND_HALF_UP);
				return result;
			}
		}

		@Override
		public Class<DecimalNode> getReturnType() {
			return DecimalNode.class;
		}
	}

	private abstract static class DoubleEvaluator implements NumberEvaluator<DoubleNode> {
		protected abstract double evaluate(double left, double right);

		@Override
		public void evaluate(final INumericNode left, final INumericNode right, DoubleNode numericTarget) {
			numericTarget.setValue(this.evaluate(left.getDoubleValue(), right.getDoubleValue()));
		}

		@Override
		public Class<DoubleNode> getReturnType() {
			return DoubleNode.class;
		}
	}

	private abstract static class IntegerEvaluator implements NumberEvaluator<IntNode> {
		protected abstract int evaluate(int left, int right);

		@Override
		public void evaluate(final INumericNode left, final INumericNode right, IntNode numericTarget) {
			numericTarget.setValue(this.evaluate(left.getIntValue(), right.getIntValue()));
		}

		@Override
		public Class<IntNode> getReturnType() {
			return IntNode.class;
		}
	}

	private abstract static class LongEvaluator implements NumberEvaluator<LongNode> {
		protected abstract long evaluate(long left, long right);

		@Override
		public void evaluate(final INumericNode left, final INumericNode right, LongNode numericTarget) {
			numericTarget.setValue(this.evaluate(left.getLongValue(), right.getLongValue()));
		}

		@Override
		public Class<LongNode> getReturnType() {
			return LongNode.class;
		}
	}

	private static interface NumberEvaluator<ReturnType extends INumericNode> {
		public void evaluate(INumericNode left, INumericNode right, ReturnType numericTarget);

		public Class<ReturnType> getReturnType();
	}
}