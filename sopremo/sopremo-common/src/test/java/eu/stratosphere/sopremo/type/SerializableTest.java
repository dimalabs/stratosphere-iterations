package eu.stratosphere.sopremo.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonParser;

public class SerializableTest {

	private static IObjectNode obj;

	private ByteArrayOutputStream byteArray;

	private DataOutputStream outStream;

	@BeforeClass
	public static void setUpClass() {
		obj = new ObjectNode();
		final IArrayNode friends = new ArrayNode();

		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 1")).put("age", IntNode.valueOf(20))
			.put("male", BooleanNode.TRUE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(30))
			.put("male", BooleanNode.FALSE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(40))
			.put("male", NullNode.getInstance()));

		obj.put("name", TextNode.valueOf("Person 1")).put("age", IntNode.valueOf(25)).put("male", BooleanNode.TRUE)
			.put("friends", friends);
	}

	@Before
	public void setUp() {
		this.byteArray = new ByteArrayOutputStream();
		this.outStream = new DataOutputStream(this.byteArray);
	}

	@Test
	public void shouldDeAndSerializeNodesCorrectly() {
		try {
			obj.write(this.outStream);
		} catch (final IOException e) {
			e.printStackTrace();
		}

		final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
		final DataInputStream inStream = new DataInputStream(byteArrayIn);

		final IObjectNode target = new ObjectNode();

		try {
			target.read(inStream);
			inStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@Test
	public void shouldDeAndSerializeEmptyNode() {
		obj = new ObjectNode();

		try {
			obj.write(this.outStream);
		} catch (final IOException e) {
			e.printStackTrace();
		}

		final IObjectNode target = new ObjectNode();

		final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
		final DataInputStream inStream = new DataInputStream(byteArrayIn);

		try {
			target.read(inStream);
			inStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@Test
	public void shouldSerializeAndDeserializeGivenFile() {

		final IArrayNode array = new ArrayNode();

		try {
			final JsonParser parser = new JsonParser(new URL(
				SopremoTest.getResourcePath("SopremoTestPlan/test.json")));
			final File file = File.createTempFile("test", "json");

			while (!parser.checkEnd())
				array.add(parser.readValueAsTree());

			array.write(this.outStream);

			final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
			final DataInputStream inStream = new DataInputStream(byteArrayIn);

			final IArrayNode target = new ArrayNode();
			target.read(inStream);

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

	@After
	public void tearDown() {
		try {
			this.outStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
