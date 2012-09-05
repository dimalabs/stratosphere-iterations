package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FormatUtil;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.serialization.DirectSchema;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * Tests {@link JsonInputFormat}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormatTest {
	/**
	 * 
	 */
	private static final DirectSchema SCHEMA = new DirectSchema();

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	@Ignore
	public void completeTestPasses() throws IOException {
		final Source read = new Source(JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"));

		final SopremoTestPlan testPlan = new SopremoTestPlan(read);
		testPlan.run();
		Assert.assertEquals("input and output should be equal in identity map", testPlan.getInput(0), testPlan
			.getActualOutput(0));
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	@Ignore
	public void completeTestPassesWithExpectedValues() throws IOException {
		final Source read = new Source(JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"));

		final SopremoTestPlan testPlan = new SopremoTestPlan(read);
		testPlan.getExpectedOutput(0).load(this.getResource("SopremoTestPlan/test.json"));
		testPlan.run();
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadArray() throws IOException {
		final File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		final OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]");
		jsonWriter.close();

		Configuration config = new Configuration();
		final EvaluationContext context = new EvaluationContext();
		context.setSchema(SCHEMA);
		SopremoUtil.serialize(config, SopremoUtil.CONTEXT, context);
		final JsonInputFormat inputFormat =
			FormatUtil.openInput(JsonInputFormat.class, file.toURI().toString(), config);
		final PactRecord record = new PactRecord();
		for (int index = 1; index <= 5; index++) {
			Assert.assertFalse("more pairs expected @ " + index, inputFormat.reachedEnd());
			Assert.assertTrue("valid pair expected @ " + index, inputFormat.nextRecord(record));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((IObjectNode) SCHEMA.recordToJson(record, null)).get("id")).getIntValue());
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more pairs but reachedEnd did not return false", inputFormat.nextRecord(record));
			Assert.fail("pair unexpected: " + SCHEMA.recordToJson(record, null));
		}
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadSingleValue() throws IOException {
		final File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		final OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("{\"array\": [{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]}");
		jsonWriter.close();

		Configuration config = new Configuration();
		final EvaluationContext context = new EvaluationContext();
		context.setSchema(SCHEMA);
		SopremoUtil.serialize(config, SopremoUtil.CONTEXT, context);
		final JsonInputFormat inputFormat =
			FormatUtil.openInput(JsonInputFormat.class, file.toURI().toString(), config);
		final PactRecord record = new PactRecord();

		if (!inputFormat.reachedEnd())
			if (!inputFormat.nextRecord(record))
				Assert.fail("one value expected expected: " + SCHEMA.recordToJson(record, null));

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more values but reachedEnd did not return false", inputFormat.nextRecord(record));
			Assert.fail("value unexpected: " + SCHEMA.recordToJson(record, null));
		}

		final IJsonNode arrayNode = ((IObjectNode) SCHEMA.recordToJson(record, null)).get("array");
		Assert.assertNotNull("could not find top level node", arrayNode);
		for (int index = 1; index <= 5; index++) {
			Assert.assertNotNull("could not find array element " + index, ((IArrayNode) arrayNode).get(index - 1));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((IObjectNode) ((IArrayNode) arrayNode).get(index - 1)).get("id")).getIntValue());
		}
	}
}
