/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dstream.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext
import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.ShuffledRDDPartition
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.SparkEnv
import io.dstream.utils.ReflectionUtils
import org.apache.spark.Aggregator
import io.dstream.SerializableStreamAssets.SerFunction
import scala.collection.JavaConversions._
import java.util.stream.StreamSupport
import java.util.Spliterators
import java.util.Spliterator
import java.util.stream.Collectors
import io.dstream.utils.KVUtils
import java.util.Map.Entry

/**
 * @author ozhurakousky
 */
class ShuffledStreamRDD(
      parent: RDD[Product2[Object, Object]],
      streamFunc:SerFunction[java.util.stream.Stream[Entry[Object,_ <: java.util.Iterator[_]]], java.util.stream.Stream[_]], 
      part: Partitioner)
    extends ShuffledRDD[Object, Object, Object](parent, part) {
  
 
  /**
   * 
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(Object, Object)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[Object, Object, Object]]
    val reader = SparkEnv.get.shuffleManager.getReader[Object, Object](dep.shuffleHandle, split.index, split.index + 1, context)
    
    val shuffleIterator = asJavaIterator(reader.read)
    
    val sourceStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(shuffleIterator, Spliterator.ORDERED), false);
    
    val groupedStream = SparkAdapters.groupTuple2Values(sourceStream)
    
    val resultStream = streamFunc(groupedStream)
    
    asScalaIterator[Entry[Object, Object]](resultStream.iterator().asInstanceOf[java.util.Iterator[Entry[Object, Object]]]).map(entry => (entry.getKey, entry.getValue))
  }
  
}