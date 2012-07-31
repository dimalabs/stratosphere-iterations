package eu.stratosphere.sopremo;

import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.AbstractJsonNode.Type;
import eu.stratosphere.sopremo.type.AbstractNumericNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;

public final class NumberCoercer {
	// private static final int NUMBER_TYPES_COUNT = IJsonNode.TYPES.values().length;

	/**
	 * The default instance.
	 */
	public static final NumberCoercer INSTANCE = new NumberCoercer();

	// private final NumberType[][] typeCoerceMatrix = new NumberType[NUMBER_TYPES_COUNT][NUMBER_TYPES_COUNT];
	//
	private final Map<AbstractJsonNode.Type, Coercer> coercers = new EnumMap<AbstractJsonNode.Type, Coercer>(
		AbstractJsonNode.Type.class);

	//
	private final Map<Class<? extends IJsonNode>, Coercer> classCoercers =
		new IdentityHashMap<Class<? extends IJsonNode>, Coercer>();

	//
	// private final Map<NumberType, Class<? extends IJsonNode>> implementationTypes = new EnumMap<NumberType, Class<?
	// extends IJsonNode>>(
	// NumberType.class);
	//
	// private final Map<Class<? extends IJsonNode>, NumberType> numberTypes = new IdentityHashMap<Class<? extends
	// IJsonNode>, NumberType>();

	public NumberCoercer() {
		// final List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT,
		// NumberType.BIG_DECIMAL,
		// NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);
		// @SuppressWarnings("unchecked")
		// final Class<? extends IJsonNode>[] types = (Class<? extends IJsonNode>[]) new Class<?>[] { DoubleNode.class,
		// DoubleNode.class, DecimalNode.class, BigIntegerNode.class, LongNode.class, IntNode.class };
		//
		// for (int index = 0; index < types.length; index++) {
		// this.implementationTypes.put(widestTypes.get(index), types[index]);
		// this.numberTypes.put(types[index], widestTypes.get(index));
		// }
		//
		// for (int leftIndex = 0; leftIndex < NUMBER_TYPES_COUNT; leftIndex++)
		// for (int rightIndex = 0; rightIndex < NUMBER_TYPES_COUNT; rightIndex++) {
		// final int coerceIndex = Math.min(widestTypes.indexOf(NumberType.values()[leftIndex]),
		// widestTypes.indexOf(NumberType.values()[rightIndex]));
		// this.typeCoerceMatrix[leftIndex][rightIndex] = widestTypes.get(coerceIndex);
		// }

		this.coercers.put(AbstractJsonNode.Type.IntNode, new Coercer() {
			@Override
			public AbstractNumericNode coerce(final IJsonNode node) {
				return IntNode.valueOf(((INumericNode) node).getIntValue());
			}
		});
		this.coercers.put(AbstractJsonNode.Type.LongNode, new Coercer() {
			@Override
			public AbstractNumericNode coerce(final IJsonNode node) {
				return LongNode.valueOf(((INumericNode) node).getLongValue());
			}
		});
		this.coercers.put(AbstractJsonNode.Type.DoubleNode, new Coercer() {
			@Override
			public AbstractNumericNode coerce(final IJsonNode node) {
				return DoubleNode.valueOf(((INumericNode) node).getDoubleValue());
			}
		});
		this.coercers.put(AbstractJsonNode.Type.BigIntegerNode, new Coercer() {
			@Override
			public AbstractNumericNode coerce(final IJsonNode node) {
				return BigIntegerNode.valueOf(((INumericNode) node).getBigIntegerValue());
			}
		});
		this.coercers.put(AbstractJsonNode.Type.DecimalNode, new Coercer() {
			@Override
			public AbstractNumericNode coerce(final IJsonNode node) {
				return DecimalNode.valueOf(((INumericNode) node).getDecimalValue());
			}
		});

		for (final Entry<AbstractJsonNode.Type, Coercer> entry : this.coercers.entrySet())
			this.classCoercers.put(entry.getKey().getClazz(), entry.getValue());
	}

	@SuppressWarnings("unchecked")
	public <T extends INumericNode> T coerce(final AbstractNumericNode node, final Class<T> targetType) {
		if (node.getClass() == targetType)
			return (T) node;
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	public INumericNode coerce(final AbstractNumericNode node, final AbstractJsonNode.Type targetType) {
		if (node.getType() == targetType)
			return node;
		return this.coercers.get(targetType).coerce(node);
	}

	@SuppressWarnings("unchecked")
	<T extends IJsonNode> T coerceGeneric(final IJsonNode node, final Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	IJsonNode coerceGeneric(final IJsonNode node, final int targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	Map<Class<? extends IJsonNode>, Coercer> getClassCoercers() {
		return this.classCoercers;
	}

	// public NumberType getWiderType(final NumberType leftType, final NumberType rightType) {
	// return this.typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	// }

	public Type getWiderType(final IJsonNode leftType, final IJsonNode rightType) {
		return leftType.getType().ordinal() >= rightType.getType().ordinal() ? leftType.getType() : rightType.getType();
	}

	private static interface Coercer extends TypeCoercer.Coercer {
		@Override
		public AbstractNumericNode coerce(IJsonNode node);
	}
}
