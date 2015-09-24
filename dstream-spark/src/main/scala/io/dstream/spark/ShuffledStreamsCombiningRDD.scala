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

import org.apache.spark.OneToOneDependency
import org.apache.spark.TaskContext
import org.apache.spark.SparkEnv
import org.apache.spark.util.collection.AppendOnlyMap
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.rdd.RDD
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.Partition
import org.apache.spark.Dependency
import io.dstream.SerializableStreamAssets.SerFunction
import scala.collection.JavaConversions._
import java.util.Map.Entry
import java.util.stream.StreamSupport
import java.util.Spliterators
import java.util.Spliterator
import java.util.stream.Collectors
import java.io.IOException
import java.io.ObjectOutputStream


private class CoGroupPartition(
    idx: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

private case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit =  {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * modeled after CoGroupedRDD
 */
class ShuffledStreamsCombiningRDD(@transient var rdds: Seq[RDD[_ <: Product2[_, _]]], 
    streamFunc:SerFunction[java.util.stream.Stream[java.util.stream.Stream[_]], java.util.stream.Stream[_]], 
    part: Partitioner)
  extends RDD[Any](rdds.head.context, Nil) {

  /**
   * 
   */
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[Any, Any, Any](
          rdd.asInstanceOf[RDD[_ <: Product2[_, _]]], part)
      }
    }
  }

  /**
   * 
   */
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.length) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  /**
   * 
   */
  override val partitioner: Some[Partitioner] = Some(part)

  /**
   * 
   */
  override def compute(s: Partition, context: TaskContext): Iterator[Any] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[Any, Any]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[Product2[Any, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(shuffleDependency.shuffleHandle, s.index, s.index + 1, context)
          .read()
        rddIterators += ((it, depNum))
    }
   
    val inputIterators = for ((it, depNum) <- rddIterators) yield asJavaIterator(it.asInstanceOf[Iterator[Product2[Object, Object]]])
    val resultStream = SparkAdapters.combineStreams(inputIterators.toArray, streamFunc)
    
    resultStream.iterator().map {element  =>
      if (element.isInstanceOf[Entry[_,_]]){
        val entry = element.asInstanceOf[Entry[_,_]]
        (entry.getKey, entry.getValue)
      }
      else {
        element
      }
    }
  }

  /**
   * 
   */
  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}