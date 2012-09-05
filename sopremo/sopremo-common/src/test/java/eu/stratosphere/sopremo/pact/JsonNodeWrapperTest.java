package eu.stratosphere.sopremo.pact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNodeTest;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class JsonNodeWrapperTest extends JsonNodeTest<JsonNodeWrapper> {

	private static IObjectNode obj;

	private ByteArrayOutputStream byteArray;

	private DataOutputStream outStream;

	@BeforeClass
	public static void setUpClass() {
		obj = new ObjectNode();
		final ArrayNode friends = new ArrayNode();

		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 1")).put("age", IntNode.valueOf(20))
			.put("male", BooleanNode.TRUE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(30))
			.put("male", BooleanNode.FALSE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(40))
			.put("male", NullNode.getInstance()));

		obj.put("name", TextNode.valueOf("Person 1")).put("age", IntNode.valueOf(25)).put("male", BooleanNode.TRUE)
			.put("friends", friends);
	}

	@Override
	@Before
	public void setUp() {
		this.byteArray = new ByteArrayOutputStream();
		this.outStream = new DataOutputStream(this.byteArray);
		this.node = new JsonNodeWrapper(obj);
	}

	@Test
	public void shouldSerializeAndDeserialize() {
		try {

			final ArrayNode array = new ArrayNode();
			final JsonParser parser = new JsonParser(new URL(
				SopremoTest.getResourcePath("SopremoTestPlan/test.json")));
			final File file = File.createTempFile("test", "json");

			while (!parser.checkEnd())
				array.add(parser.readValueAsTree());

			SopremoUtil.wrap(array).write(this.outStream);

			final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
			final DataInputStream inStream = new DataInputStream(byteArrayIn);

			final JsonNodeWrapper wrapper = new JsonNodeWrapper();
			wrapper.read(inStream);
			final IJsonNode target = SopremoUtil.unwrap(wrapper);

			// for watching the output
			final JsonGenerator gen = new JsonGenerator(file);
			gen.writeStartArray();
			gen.writeTree(target);
			gen.writeEndArray();
			gen.close();

			Assert.assertEquals(array, target);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void shouldNotEqualWithNonWrapperNodes() {
		Assert.assertFalse(this.node.equals(IntNode.valueOf(42)));
	}

	@Test
	public void shouldNotEqualWithNull() {
		Assert.assertFalse(this.node.equals(null));
	}

	@Test
	public void shouldEqualWithNullIfValueIsNull() {
		this.node = new JsonNodeWrapper(null);
		Assert.assertTrue(this.node.equals(new JsonNodeWrapper(null)));
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		return new JsonNodeWrapper(TextNode.valueOf("1 lower node"));
	}

	@Override
	protected IJsonNode higherNode() {
		return new JsonNodeWrapper(TextNode.valueOf("2 higher node"));
	}
}
