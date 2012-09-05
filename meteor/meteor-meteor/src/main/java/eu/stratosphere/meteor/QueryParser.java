package eu.stratosphere.meteor;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;

import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.packages.IRegistry;
import eu.stratosphere.sopremo.query.OperatorInfo;
import eu.stratosphere.sopremo.query.OperatorInfo.InputPropertyInfo;
import eu.stratosphere.sopremo.query.OperatorInfo.OperatorPropertyInfo;
import eu.stratosphere.sopremo.query.IOperatorRegistry;
import eu.stratosphere.sopremo.query.PlanCreator;
import eu.stratosphere.sopremo.query.QueryParserException;
import eu.stratosphere.util.StringUtil;

public class QueryParser extends PlanCreator {

	Deque<List<Operator<?>>> operatorInputs = new LinkedList<List<Operator<?>>>();

	@Override
	public SopremoPlan getPlan(final InputStream stream) {
		try {
			return this.tryParse(stream);
		} catch (final Exception e) {
			return null;
		}
	}

	public SopremoPlan tryParse(final InputStream stream) throws IOException, QueryParserException {
		return this.tryParse(new ANTLRInputStream(stream));
	}

	public SopremoPlan tryParse(final String script) throws QueryParserException {
		return this.tryParse(new ANTLRStringStream(script));
	}

	protected SopremoPlan tryParse(final CharStream tryParse) {
		final MeteorLexer lexer = new MeteorLexer(tryParse);
		final CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		final MeteorParser parser = new MeteorParser(tokens);
		parser.setTreeAdaptor(new SopremoTreeAdaptor());
		return parser.parse();
	}

	public static String getPrefixedName(String prefix, String name) {
		return String.format("%s:%s", prefix, name);
	}

	public String toSopremoCode(final InputStream stream) throws IOException, QueryParserException {
		return this.toSopremoCode(new ANTLRInputStream(stream));
	}

	public String toSopremoCode(final String script) throws QueryParserException {
		return this.toSopremoCode(new ANTLRStringStream(script));
	}

	protected String toSopremoCode(final CharStream input) {
		final MeteorLexer lexer = new MeteorLexer(input);
		final CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		final MeteorParser parser = new MeteorParser(tokens);
		final TraceableSopremoTreeAdaptor adaptor = new TraceableSopremoTreeAdaptor();
		parser.setTreeAdaptor(adaptor);
		final SopremoPlan result = parser.parse();
		final JavaRenderInfo info = new JavaRenderInfo(parser, adaptor);
		this.toSopremoCode(result, info);
		return info.builder.toString();
	}

	protected String toSopremoCode(final SopremoPlan result, final JavaRenderInfo info) {
		for (final Operator<?> op : result.getContainedOperators())
			this.appendJavaOperator(op, info);
		return info.builder.toString();
	}

	@SuppressWarnings("unchecked")
	protected <O extends Operator<O>> void appendJavaOperator(final Operator<O> op, final JavaRenderInfo renderInfo) {
		final String className = op.getClass().getSimpleName();
		renderInfo.builder.append(String.format("%s %s = new %1$s();\n", className, renderInfo.getVariableName(op)));

		final IOperatorRegistry operatorRegistry = renderInfo.parser.getOperatorRegistry();
		final String name = operatorRegistry.getName((Class<O>) op.getClass());
		final OperatorInfo<O> info = (OperatorInfo<O>) operatorRegistry.get(name);
		final Operator<O> defaultInstance = info.newInstance();
		this.appendInputs(op, renderInfo, defaultInstance);
		defaultInstance.setInputs(op.getInputs());
		this.appendOperatorProperties(op, renderInfo, info, defaultInstance);
		this.appendInputProperties(op, renderInfo, info, defaultInstance);
	}

	protected <O extends Operator<O>> void appendInputs(final Operator<O> op, final JavaRenderInfo renderInfo,
			final Operator<O> defaultInstance) {
		if (!defaultInstance.getInputs().equals(op.getInputs())) {
			renderInfo.builder.append(renderInfo.getVariableName(op)).append(".setInputs(");
			for (int index = 0; index < op.getInputs().size(); index++) {
				if (index > 0)
					renderInfo.builder.append(", ");
				renderInfo.builder.append(renderInfo.getVariableName(op.getInput(index)));
			}
			renderInfo.builder.append(");\n");
		}
	}

	protected <O extends Operator<O>> void appendInputProperties(final Operator<O> op, final JavaRenderInfo renderInfo,
			final OperatorInfo<O> info, final Operator<O> defaultInstance) {
		final IRegistry<InputPropertyInfo> inputPropertyRegistry = info.getInputPropertyRegistry();
		for (final String propertyName : inputPropertyRegistry.keySet())
			for (int index = 0; index < op.getInputs().size(); index++) {
				final InputPropertyInfo propertyInfo = inputPropertyRegistry.get(propertyName);
				final Object actualValue = propertyInfo.getValue(op, index);
				final Object defaultValue = propertyInfo.getValue(defaultInstance, index);
				if (!actualValue.equals(defaultValue)) {
					renderInfo.builder.append(renderInfo.getVariableName(op)).
						append(".set").append(StringUtil.upperFirstChar(propertyInfo.getDescriptor().getName())).
						append("(").append(index).append(", ");
					this.appendExpression(actualValue, renderInfo);
					renderInfo.builder.append(");\n");
				}
			}
	}

	private void appendExpression(final Object value, final JavaRenderInfo renderInfo) {
		renderInfo.adaptor.addJavaFragment(value, renderInfo.builder);
	}

	protected <O extends Operator<O>> void appendOperatorProperties(final Operator<O> op,
			final JavaRenderInfo renderInfo, final OperatorInfo<O> info, final Operator<O> defaultInstance) {
		
		final IRegistry<OperatorPropertyInfo> operatorPropertyRegistry = info.getOperatorPropertyRegistry();
		for (final String propertyName : operatorPropertyRegistry.keySet()) {
			final OperatorPropertyInfo propertyInfo = operatorPropertyRegistry.get(propertyName);
			final Object actualValue = propertyInfo.getValue(op);
			final Object defaultValue = propertyInfo.getValue(defaultInstance);
			if (!actualValue.equals(defaultValue)) {
				renderInfo.builder.append(renderInfo.getVariableName(op)).
					append(".set").append(StringUtil.upperFirstChar(propertyInfo.getDescriptor().getName())).
					append("(");
				this.appendExpression(actualValue, renderInfo);
				renderInfo.builder.append(");\n");
			}
		}
	}

	private static class JavaRenderInfo {
		private final MeteorParser parser;

		private final TraceableSopremoTreeAdaptor adaptor;

		private final StringBuilder builder = new StringBuilder();

		private final Map<JsonStream, String> variableNames = new IdentityHashMap<JsonStream, String>();

		public JavaRenderInfo(final MeteorParser parser, final TraceableSopremoTreeAdaptor adaptor) {
			this.parser = parser;
			this.adaptor = adaptor;
		}

		public String getVariableName(final JsonStream input) {
			final Operator<?> op = input instanceof Operator ? (Operator<?>) input : input.getSource().getOperator();
			String name = this.variableNames.get(op);
			if (name == null) {
				final int counter = this.instanceCounter.getInt(op.getClass()) + 1;
				this.instanceCounter.put(op.getClass(), counter);
				name = String.format("%s%d", StringUtil.lowerFirstChar(op.getClass().getSimpleName()), counter);
				this.variableNames.put(op, name);
			}
			return name;
		}

		private final Object2IntMap<Class<?>> instanceCounter = new Object2IntOpenHashMap<Class<?>>();
	}
}
