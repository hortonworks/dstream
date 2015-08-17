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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import dstream.function.SerializableFunctionConverters.SerFunction;

/**
 * Implementation of {@link SerFunction} which will union multiple streams
 * while applying user functionality at check points (see this{@link #addCheckPoint(int)}.
 */
public class StreamUnionFunction extends AbstractMultiStreamProcessingFunction {
	private static final long serialVersionUID = -2955908820407886806L;
	
	private final boolean distinct;
	
	/**
	 * Constructs this function.
	 * 
	 * @param distinct boolean signaling if union results should be distinct, 
	 *  essentially supporting the standard <i>union</i> and <i>unionAll</i> semantics.
	 */
	public StreamUnionFunction(boolean distinct, SerFunction<Stream<?>, Stream<?>> firstStreamPreProcessingFunction){
		super(firstStreamPreProcessingFunction);
		this.distinct = distinct;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<?> doApply(List<Stream<?>> streamsList) {	

		AtomicInteger ctr = new AtomicInteger(2); 
		
		Stream<?> unionizedStream = streamsList.stream()
				.reduce((lStream,rStream) -> {
					Stream<?> newStream = Stream.concat(lStream,rStream);
					int currentStreamIdx = ctr.getAndIncrement();
					for (int j = 0; j < checkPointProcedures.size(); j++) {
						Object[] postProc = checkPointProcedures.get(j);
						if ((Integer)postProc[0] == currentStreamIdx){
							SerFunction f = (SerFunction) postProc[1];
							if (f != null){
								newStream = (Stream) f.apply(newStream);
							}
						}
					}
					return newStream;
				}).get();
		
		if (this.distinct){
			unionizedStream = unionizedStream.distinct();
		}
		
		return unionizedStream;
	}
}
