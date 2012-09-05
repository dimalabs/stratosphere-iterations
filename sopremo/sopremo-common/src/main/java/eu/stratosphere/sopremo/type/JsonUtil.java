package eu.stratosphere.sopremo.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;

/**
 * Provides a set of utility functions and objects to handle json data.
 * 
 * @author Arvid Heise
 */
public class JsonUtil {
	/**
	 * A general purpose {@link ObjectMapper}. No state of this mapper should be changed. If a specifically configured
	 * ObjectMapper is needed, a new instance should be created.
	 */
	public static final JavaToJsonMapper OBJECT_MAPPER = new JavaToJsonMapper();

	// /**
	// * A general purpose {@link JsonNodeFactory}. No state of this node factory should be changed. If a specifically
	// * configured JsonNodeFactory is needed, a new instance should be created.
	// */
	// public static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	// /**
	// * A general purpose {@link JsonFactory}. No state of this factory should be changed. If a specifically
	// * configured JsonFactory is needed, a new instance should be created.
	// */
	// public static final JsonFactory FACTORY = new JsonFactory();


	/**
	 * Creates an efficient read-only wrapper for the given node array.
	 * 
	 * @param nodes
	 *        the nodes to wrap
	 * @return an efficient wrapper
	 */
	public static IArrayNode asArray(final IJsonNode... nodes) {
		return new ArrayNode(nodes);
	}

	/**
	 * Wraps streams of objects in a {@link JsonNode}. This is the unsafe equivalent of
	 * {@link #wrapWithNode(boolean, List)}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterators
	 *        the streams of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the streams
	 * @see #wrapWithNode(boolean, List)
	 */
	@SuppressWarnings("unchecked")
	public static IJsonNode wrapWithNode(final boolean resettable, final Iterator<?>... objectIterators) {
		final AbstractJsonNode[] streamNodes = new AbstractJsonNode[objectIterators.length];
		for (int index = 0; index < streamNodes.length; index++)
			streamNodes[index] = wrapWithNode(resettable, (Iterator<IJsonNode>) objectIterators[index]);
		return new ArrayNode(streamNodes);
	}

	/**
	 * Wraps a stream of objects in a {@link JsonNode}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterator
	 *        the stream of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the stream
	 */
	public static ArrayNode wrapWithNode(@SuppressWarnings("unused") final boolean resettable,
			final Iterator<IJsonNode> objectIterator) {
		return ArrayNode.valueOf(objectIterator/* , resettable */);
	}

	/**
	 * Wraps streams of objects in a {@link JsonNode}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterators
	 *        the streams of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the streams
	 */
	public static IJsonNode wrapWithNode(final boolean resettable,
			final List<? extends Iterator<AbstractJsonNode>> objectIterators) {
		final IJsonNode[] streamNodes = new AbstractJsonNode[objectIterators.size()];
		for (int index = 0; index < streamNodes.length; index++)
			streamNodes[index] = wrapWithNode(resettable, objectIterators.get(index));
		return new ArrayNode(streamNodes);
	}

	/**
	 * Creates a new {@link PathExpression} from the given parts.
	 * 
	 * @param parts
	 *        the parts that should be used
	 * @return the expression
	 */
	public static EvaluationExpression createPath(final List<String> parts) {
		final List<EvaluationExpression> fragments = new ArrayList<EvaluationExpression>();
		for (int index = 0; index < parts.size(); index++) {
			EvaluationExpression segment;
			final String part = parts.get(index);
			if (part.matches("[0-9]+"))
				segment = new InputSelection(Integer.parseInt(part));
			else if (part.matches("\\[.*\\]")) {
				if (part.charAt(1) == '*') {
					segment = new ArrayProjection(createPath(parts.subList(index + 1, parts.size())));
					index = parts.size();
				} else if (part.contains(":")) {
					final int delim = part.indexOf(":");
					segment = new ArrayAccess(Integer.parseInt(part.substring(1, delim)),
						Integer.parseInt(part.substring(delim + 1, part.length() - 1)));
				} else
					segment = new ArrayAccess(Integer.parseInt(part.substring(1, part.length() - 1)));
			} else
				segment = new ObjectAccess(part);
			fragments.add(segment);
		}
		return PathExpression.wrapIfNecessary(fragments);
	}

	/**
	 * Creates a new {@link PathExpression} from the given parts.
	 * 
	 * @param parts
	 *        the parts that should be used
	 * @return the expression
	 */
	public static EvaluationExpression createPath(final String... parts) {
		return createPath(Arrays.asList(parts));
	}

	/**
	 * Creates an {@link ArrayNode} that contains all given constants as elements. This method converts the whole array
	 * of parameters to an ArrayNode.
	 * 
	 * @param constants
	 *        the constants that should be used to fill the array
	 * @return the array
	 */
	public static ArrayNode createArrayNode(final Object... constants) {
		return (ArrayNode) JsonUtil.OBJECT_MAPPER.valueToTree(constants);
	}

	/**
	 * Creates an {@link ArrayNode} that contains all given constants as elements. This method converts each parameter
	 * to an appropriate JsonNode before creating the ArrayNode.
	 * 
	 * @param constants
	 *        the constants that should be used to fill the array
	 * @return the array
	 * @param constants
	 * @return
	 */
	public static IArrayNode createCompactArray(final Object... constants) {
		final IJsonNode[] nodes = new IJsonNode[constants.length];
		for (int index = 0; index < nodes.length; index++)
			nodes[index] = createValueNode(constants[index]);
		return JsonUtil.asArray(nodes);
	}

	/**
	 * Creates an {@link ObjectNode} that contains all given fields with there related value. The key and value of each
	 * field has do be specified like the following example:</br>
	 * createObjectNode("key1", value1, "key2", value2, ...)
	 * 
	 * @param fields
	 *        the key-value pairs that should be used
	 * @return the object node
	 */
	public static ObjectNode createObjectNode(final Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException("must have an even number of params");
		final ObjectNode objectNode = new ObjectNode();
		for (int index = 0; index < fields.length; index += 2)
			objectNode.put(fields[index].toString(), JsonUtil.OBJECT_MAPPER.valueToTree(fields[index + 1]));
		return objectNode;
	}

	public static IJsonNode createValueNode(final Object value) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(value);
	}

}
