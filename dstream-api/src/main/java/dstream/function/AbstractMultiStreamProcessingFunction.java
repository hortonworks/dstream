/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.function;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.SerializableFunctionConverters.SerPredicate;
import dstream.utils.Assert;

/**
 * Base implementation of {@link SerFunction} for multi-stream processing.
 * See {@link StreamJoinerFunction} and {@link StreamUnionFunction} for concrete 
 * implementation.
 *
 */
public abstract class AbstractMultiStreamProcessingFunction implements SerFunction<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -7336517082191905937L;

	protected List<Object[]> checkPointProcedures = new ArrayList<>();
	
	private int streamCounter = 1;
	
	private final SerFunction<Stream<?>, Stream<?>> firstStreamPreProcessingFunction;
	
	public AbstractMultiStreamProcessingFunction(SerFunction<Stream<?>, Stream<?>> firstStreamPreProcessingFunction){
		this.firstStreamPreProcessingFunction = firstStreamPreProcessingFunction;
	}
	
	@Override
	public Stream<?> apply(Stream<Stream<?>> streams) {	
		Assert.notNull(streams, "'streams' must not be null");
		
		List<Stream<?>> streamsList = streams.collect(Collectors.toList());	
		
		Stream<?> firstStream = streamsList.get(0);
		firstStream = this.firstStreamPreProcessingFunction.apply(firstStream);
		streamsList.set(0, firstStream);
		
//		Stream<?> secondStream = streamsList.get(1);
//		secondStream = this.firstStreamPreProcessingFunction.apply(secondStream);
//		streamsList.set(1, secondStream);
		
		
		return this.doApply(streamsList);
	}
	
	protected abstract Stream<?> doApply(List<Stream<?>> streams);
	/**
	 * Will add a check point at which additional functionality may be provided before 
	 * proceeding with the join.<br>
	 * For example:
	 * <pre>
	 * 	DStream[A].join(DStream2[B,C]).map(s -> . . .).join(DStream[D]).on(Predicate)
	 * </pre>
	 * The above represents 4-way join where upon joining of 3 streams 
	 * (1 + 2 = 3; DStream[A] - 1 and DStream[B,C] - 2) a mapping function needs to 
	 * be applied before proceeding with the fourth.<br>
	 * In the above case the value of 'joiningStreamsCount' is 2 at the first check 
	 * point and 1 at the second(last).
	 * 
	 * @param joiningStreamsCount
	 */
	public void addCheckPoint(int joiningStreamsCount){
		this.streamCounter += joiningStreamsCount;
		Object[] procedure = new Object[2];
		procedure[0] = this.streamCounter;
		this.checkPointProcedures.add(procedure);
	}
	
	/**
	 * Will add transformation ({@link SerFunction}) or predicate ({@link SerPredicate}) to be 
	 * applied to the last (current) checkpoint.<br>
	 * To ensure that both (transformation and predicate) cold be represented as a {@link SerFunction}, 
	 * this method will wrap provided 'transformationOrPredicate' with {@link DStreamToStreamAdapterFunction}.
	 * 
	 * @param operationName
	 * @param transformationOrPredicate
	 */
	public void addTransformationOrPredicate(String operationName, Object transformationOrPredicate) {
		DStreamToStreamAdapterFunction incomingFunc = new DStreamToStreamAdapterFunction(operationName, transformationOrPredicate);
		this.addTransformationOrPredicate(incomingFunc);
	}
	
	/**
	 * Will add transformation ({@link SerFunction}) or predicate ({@link SerPredicate}) to be 
	 * applied to the last (current) checkpoint.
	 *  
	 * @param transformationOrPredicate
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addTransformationOrPredicate(SerFunction transformationOrPredicate) {
		Object[] currentProcedure = this.checkPointProcedures.get(this.checkPointProcedures.size()-1);
		
		SerFunction func = (SerFunction) currentProcedure[1] == null 
				? transformationOrPredicate 
						: transformationOrPredicate.compose((SerFunction) currentProcedure[1]);
		
		currentProcedure[1] = func;
	}
}
