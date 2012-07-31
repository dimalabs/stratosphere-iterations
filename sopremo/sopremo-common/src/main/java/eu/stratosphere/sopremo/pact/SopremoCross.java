package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class SopremoCross extends CrossStub {
	private EvaluationContext context;

	private Schema inputSchema1, inputSchema2;

	private JsonCollector collector;

	private IJsonNode cachedInput1, cachedInput2;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(final Configuration parameters) throws Exception {
		// We need to pass our class loader since the default class loader is
		// not able to resolve classes coming from the Sopremo user jar file.
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT,
			EvaluationContext.class, this.getClass().getClassLoader());
		this.inputSchema1 = this.context.getInputSchema(0);
		this.inputSchema2 = this.context.getInputSchema(1);
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected abstract void cross(IJsonNode value1, IJsonNode value2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CrossStub#cross(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void cross(final PactRecord record1, final PactRecord record2, final Collector<PactRecord> out) {
		this.context.increaseInputCounter();
		this.collector.configure(out, this.context);
		final IJsonNode input1 = this.inputSchema1.recordToJson(record1, this.cachedInput1);
		final IJsonNode input2 = this.inputSchema2.recordToJson(record2, this.cachedInput2);
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), input1, input2));
		try {
			this.cross(input1, input2, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with v1 %s/%s v2: %s", this.getContext()
				.operatorTrace(), input1, input2, e));
			throw e;
		}
	}

	protected final EvaluationContext getContext() {
		return this.context;
	}
}
