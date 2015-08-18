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

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.support.CollectionFactory;
import dstream.utils.Assert;
import dstream.utils.Tuples.Tuple;
import dstream.utils.Tuples.Tuple2;

/**
 * Implementation of {@link SerFunction} which will join multiple streams
 * while applying user functionality at check points (see this{@link #addCheckPoint(int)}.
 */
public class StreamJoinerFunction extends AbstractMultiStreamProcessingFunction {
	private static final long serialVersionUID = -3615487628958776468L;
	
	private static CollectionFactory collectionFactory;

	static {
		if (collectionFactory == null){
			Iterator<CollectionFactory> sl = ServiceLoader
		            .load(CollectionFactory.class, ClassLoader.getSystemClassLoader()).iterator();
			
			collectionFactory = sl.hasNext() ? sl.next() : null;
			if (collectionFactory == null){
				throw new IllegalStateException("Failed to find '" + CollectionFactory.class.getName() + "' provider.");
			}
		}
	}
	
	public StreamJoinerFunction(SerFunction<Stream<?>, Stream<?>> streamPreProcessingFunction) {
		super(streamPreProcessingFunction);
	}
	
	/**
	 * 
	 */
	@Override
	protected Stream<?> doApply(List<Stream<?>> streamsList) {	
		Assert.notNull(streamsList, "'streamsList' must not be null");
		Assert.isTrue(streamsList.size() >= 2, "There must be 2+ streams available to perform join. Was " + streamsList.size());
		
		Stream<?> result = this.join(streamsList);
		return result;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Stream<?> join(List<Stream<?>> streams) {
		Stream joinedStream = streams.remove(0);
		
		int streamCount = 0;
		int streamProcessedCounter = 2;
		int procedureCount = 0;
		Tuple2<Integer, Object> postJoinProcedure = null;
		do {
			if (this.checkPointProcedures.size() > 0){
				if (this.checkPointProcedures.size() > procedureCount){
					postJoinProcedure = this.checkPointProcedures.get(procedureCount++);
					streamCount = postJoinProcedure._1();
				}
			}
			joinedStream = this.doJoin(joinedStream, streams.remove(0));
			if (streamCount == streamProcessedCounter){
				SerFunction<Stream, Stream> postJoinProcedureFunction = (SerFunction) postJoinProcedure._2();
				if (postJoinProcedureFunction != null){
					joinedStream = postJoinProcedureFunction.apply(joinedStream);
				}
			}
			streamProcessedCounter++;
		} while (streams.size() > 0);
		
		return joinedStream;
	}
	
	/**
	 * 
	 */
	private Stream<?> doJoin(Stream<?> joinedStream, Stream<?> joiningStream) {
		List<Object> joiningStreamCache = collectionFactory.newList();
		return joinedStream.flatMap(lVal -> {
			boolean cached = joiningStreamCache.size() > 0;
			Stream<?> _joiningStream = cached ? joiningStreamCache.stream() : joiningStream;
			try {
				return _joiningStream.map(rVal -> {
					if (!cached){
						joiningStreamCache.add(rVal);
					}
					return this.mergeValues(lVal, rVal);
				});
			} catch (Exception e) {
				throw new IllegalStateException("Failed to join partitions. Possible reason: The system may be trying to join on an empty partition. \n"
						+ "This could happen due to the fact that your initial data was too small to be partitioned in the amount specified. \nPlease try"
						+ " to lower dstream.parallelism size. ", e);
			}
		});	
	}
	
	/**
	 * 
	 */
	private Tuple mergeValues(Object left, Object right) {
		Tuple current = left instanceof MergableTuple ? (MergableTuple)left : new MergableTuple(left);
		Tuple cloned = current.size() > 1 ? current.clone() : current;
		cloned.add(right);
		return cloned;
	}
	
	/**
	 * 
	 */
	private static class MergableTuple extends Tuple {
		private static final long serialVersionUID = 6081720376172843799L;

		MergableTuple(Object... values){
			super(values);
		}
	}
}
