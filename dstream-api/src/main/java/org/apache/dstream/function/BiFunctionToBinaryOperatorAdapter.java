package org.apache.dstream.function;

import org.apache.dstream.function.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;

@SuppressWarnings("rawtypes")
public class BiFunctionToBinaryOperatorAdapter implements BinaryOperator<Object> {
	private static final long serialVersionUID = -2240524865400623818L;
	
	
	private final BiFunction bo;
	
	public BiFunctionToBinaryOperatorAdapter(BiFunction bo){
		this.bo = bo;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object apply(Object t, Object u) {
		return this.bo.apply(t, u);
	}
}
