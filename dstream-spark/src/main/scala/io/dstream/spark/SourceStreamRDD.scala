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

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import io.dstream.SerializableStreamAssets.SerFunction
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import java.util.stream.StreamSupport
import java.util.Spliterator
import java.util.Spliterators
import java.util.stream.Collectors
import java.util.Map.Entry
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.CoGroupedRDD

/**
 * 
 */
class SourceStreamRDD[I:ClassTag, O:ClassTag](prev: RDD[_],
          streamFunc:SerFunction[java.util.stream.Stream[_], java.util.stream.Stream[Entry[Object,Object]]],  
          preservesPartitioning: Boolean = false) extends RDD[O](prev) {
  
  /**
   * 
   */
  override val partitioner = if (preservesPartitioning) firstParent[Any].partitioner else None

  /**
   * 
   */
  override def getPartitions: Array[Partition] = firstParent[Any].partitions

  /**
   * 
   */
  override def compute(split: Partition, context: TaskContext): Iterator[O] = {
    val it = firstParent[Any].iterator(split, context) 
    val resultStream = SparkAdapters.processSourceStreamRDD(it, streamFunc) 
    asScalaIterator(resultStream.iterator).asInstanceOf[Iterator[O]]
  }
}