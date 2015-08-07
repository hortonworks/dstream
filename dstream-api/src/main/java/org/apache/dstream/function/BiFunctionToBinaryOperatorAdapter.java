package org.apache.dstream.function;

import org.apache.dstream.function.SerializableFunctionConverters.SerBiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;

/**
 * A conversion utility used to de-type and represent {@link SerBiFunction} 
 * as {@link SerBinaryOperator}. Typically used to promote code re-usability when 
 * implementing support for reducing and aggregation, since in a type-less world
 * the two are the same.<br>
 * See {@link ValuesAggregatingFunction} and {@link ValuesReducingFunction} for more details 
 * on its applicability.
 */
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
