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
package io.dstream;

import static io.dstream.utils.Tuples.Tuple2.tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.SerializableStreamAssets.SerPredicate;
import io.dstream.function.DStreamToStreamAdapterFunction;
import io.dstream.utils.Assert;
import io.dstream.utils.Tuples.Tuple2;

/**
 * Base implementation of {@link SerFunction} for multi-stream processing.
 * See {@link StreamJoinerFunction} and {@link StreamUnionFunction} for concrete
 * implementation.
 *
 */
abstract class AbstractStreamMergingFunction implements SerFunction<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -7336517082191905937L;

	private final SerFunction<Stream<?>, Stream<?>> streamPreProcessingFunction;

	private int streamCounter = 1;

	protected List<Tuple2<Integer, Object>> checkPointProcedures = new ArrayList<>();

	/**
	 * Constructs new {@link AbstractStreamMergingFunction}
	 *
	 * @param streamPreProcessingFunction an instance of {@link SerFunction} which will be applied
	 * on each {@link Stream} before they are merged.
	 */
	public AbstractStreamMergingFunction(SerFunction<Stream<?>, Stream<?>> streamPreProcessingFunction){
		this.streamPreProcessingFunction = streamPreProcessingFunction;
	}

	/**
	 *
	 */
	@Override
	public Stream<?> apply(Stream<Stream<?>> streams) {
		Assert.notNull(streams, "'streams' must not be null");

		try {
			List<Stream<?>> streamsList = streams.map(stream -> streamPreProcessingFunction.apply(stream)).collect(Collectors.toList());
			return this.doApply(streamsList);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to combine streams", e);
		}
	}

	/**
	 *
	 * @param streams
	 * @return
	 */
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
		this.checkPointProcedures.add(tuple2(this.streamCounter, null));
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
		Tuple2<Integer, Object> currentProcedure = this.checkPointProcedures.get(this.checkPointProcedures.size()-1);
		SerFunction func = (SerFunction) currentProcedure._2() == null
				? transformationOrPredicate
						: transformationOrPredicate.compose((SerFunction) currentProcedure._2());
		this.checkPointProcedures.set(this.checkPointProcedures.size()-1, tuple2(currentProcedure._1(), func));
	}
}
