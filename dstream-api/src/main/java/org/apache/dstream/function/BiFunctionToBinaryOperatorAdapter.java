package org.apache.dstream.function;

import org.apache.dstream.function.SerializableFunctionConverters.SerBiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;

@SuppressWarnings("rawtypes")
public class BiFunctionToBinaryOperatorAdapter implements SerBinaryOperator<Object> {
	private static final long serialVersionUID = -2240524865400623818L;
	
	
	private final SerBiFunction bo;
	
	public BiFunctionToBinaryOperatorAdapter(SerBiFunction bo){
		this.bo = bo;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object apply(Object t, Object u) {
		return this.bo.apply(t, u);
	}
}
