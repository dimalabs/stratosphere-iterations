package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class ElementInSetExpressionTest extends EvaluableExpressionTest<ElementInSetExpression> {

	@Override
	protected ElementInSetExpression createDefaultInstance(final int index) {

		return new ElementInSetExpression(new ConstantExpression(IntNode.valueOf(index)), Quantor.EXISTS_IN,
			new ArrayCreation(new ConstantExpression(IntNode.valueOf(index))));
	}

	@Test
	public void shouldFindElementInSet() {
		final IJsonNode result = new ElementInSetExpression(new InputSelection(0), Quantor.EXISTS_IN,
			new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(2),
				createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3))), null, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldNotFindElementInSet() {
		final IJsonNode result = new ElementInSetExpression(new InputSelection(0), Quantor.EXISTS_IN,
			new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(0),
				createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3))), null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test
	public void shouldFindNonexistingElementInSet() {
		final IJsonNode result = new ElementInSetExpression(new InputSelection(0), Quantor.EXISTS_NOT_IN,
			new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(2),
				createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3))), null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test
	public void shouldNotFindNonexistingElementInSet() {
		final IJsonNode result = new ElementInSetExpression(new InputSelection(0), Quantor.EXISTS_NOT_IN,
			new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(0),
				createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3))), null, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldReturnFalseIfSetIsEmpty() {
		final IJsonNode result = new ElementInSetExpression(new InputSelection(0), Quantor.EXISTS_IN,
			new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(2),
				createArrayNode()), null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}
}
