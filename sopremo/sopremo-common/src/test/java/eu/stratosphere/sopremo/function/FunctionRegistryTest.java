package eu.stratosphere.sopremo.function;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IPrimitiveNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class FunctionRegistryTest {
	private MethodRegistry registry;

	private EvaluationContext context;

	private static final AbstractJsonNode GENERIC_VARARG_NODE = new TextNode("var");

	private static final AbstractJsonNode GENERIC_NODE = new TextNode("generic");

	private static final AbstractJsonNode ARRAY_NODE = new TextNode("array");

	private static final AbstractJsonNode TWO_INT_NODE = new TextNode("2 int");

	private static final AbstractJsonNode ONE_INT_VARARG_NODE = new TextNode("1 int + var");

	private static final AbstractJsonNode SUM_NODE = new TextNode("sum");

	@Before
	public void setup() {
		this.context = new EvaluationContext();
		this.registry = this.context.getFunctionRegistry();
	}

	@Test
	public void shouldAddJavaFunctions() {
		this.registry.register(JavaFunctions.class);

		Assert.assertEquals("should have been 2 functions", 2, this.registry.getRegisteredFunctions().size());
		for (final JsonMethod function : this.registry.getRegisteredFunctions().values())
			Assert.assertEquals("should have been a java function", JavaMethod.class, function.getClass());

		Assert.assertEquals("should have been 5 count signatures", 5,
			((JavaMethod) this.registry.getFunction("count"))
				.getSignatures().size());
		Assert.assertEquals("should have been 1 sum signatures", 1, ((JavaMethod) this.registry.getFunction("sum"))
			.getSignatures().size());
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoApproporiateMatchingJavaFunction() {
		this.registry.register(JavaFunctions.class);

		this.registry.evaluate("sum", JsonUtil.asArray(new IntNode(1),
			new IntNode(2), new TextNode("3")), this.context);
	}

	@Test
	public void shouldInvokeArrayJavaFunctionForArrayNode() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(ARRAY_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new ArrayNode()), this.context));
	}

	@Test
	public void shouldInvokeDerivedVarargJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(SUM_NODE,
			this.registry.evaluate("sum", JsonUtil.asArray(new IntNode(1),
				new IntNode(2), new IntNode(3)), this.context));
		Assert.assertSame(SUM_NODE,
			this.registry.evaluate("sum", JsonUtil.asArray(new IntNode(1)), this.context));
	}

	@Test
	public void shouldInvokeExactMatchingJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(TWO_INT_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new IntNode(1),
				new IntNode(2)), this.context));
	}

	@Test
	public void shouldInvokeFallbackJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new ObjectNode()), this.context));
	}

	@Test
	public void shouldInvokeGenericVarargJavaFunctionsIfNoExactMatch() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_VARARG_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new ObjectNode(),
				new ObjectNode(), new ObjectNode()), this.context));
	}

	@Test
	public void shouldInvokeMostSpecificVarargJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(ONE_INT_VARARG_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new IntNode(1),
				new IntNode(2), new IntNode(3)), this.context));
		Assert.assertSame(ONE_INT_VARARG_NODE,
			this.registry.evaluate("count", JsonUtil.asArray(new IntNode(1)), this.context));
	}

	@SuppressWarnings("unused")
	public static class JavaFunctions {

		public static IJsonNode count(final IArrayNode node) {
			return ARRAY_NODE;
		}

		public static IJsonNode count(final IPrimitiveNode node, final IPrimitiveNode node2) {
			return TWO_INT_NODE;
		}

		public static IJsonNode count(final IPrimitiveNode node, final IPrimitiveNode... nodes) {
			return ONE_INT_VARARG_NODE;
		}

		public static IJsonNode count(final IJsonNode node) {
			return GENERIC_NODE;
		}

		public static IJsonNode count(final IJsonNode... node) {
			return GENERIC_VARARG_NODE;
		}

		public static IJsonNode sum(final INumericNode... nodes) {
			return SUM_NODE;
		}
	}
}
