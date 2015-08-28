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
package org.apache.spark.dstream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import dstream.DStreamOperation
import dstream.DStreamConstants
import dstream.support.SourceSupplier
import java.util.Properties
import java.net.URI
import java.util.Arrays
import dstream.utils.Assert
import dstream.DStreamOperations
import scala.collection.JavaConversions._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import dstream.SerializableStreamAssets.SerFunction
import java.util.Map.Entry

/**
 * @author ozhurakousky
 */
class SparkDAGBuilder(ctx:SparkContext, executionName:String, executionConfig:Properties) {
  
  private var rdd:RDD[_] = null
  
  private val partitionCount = 2
   
  def build(ops:DStreamOperations):RDD[_] = {
    for (operation <- ops.getOperations) {
      if (rdd == null){
       val path = this.getPath(ops.getName, executionConfig, executionName)
       val sourceRDD = ctx.textFile(path) //TODO figure out how to (or even if its needed given the natural splits) pass parallelization param
       val fistFunction = operation.getStreamOperationFunction   
       val stage1RDD = new SourceStreamRDD[Object, Product2[Object, Object]](sourceRDD, 
           fistFunction.asInstanceOf[SerFunction[java.util.stream.Stream[_],java.util.stream.Stream[Entry[Object,Object]]]])
       rdd = stage1RDD
      }
      else {
        if (!operation.getCombinableStreamOperations.isEmpty()) {
          val dependentExecutionGraphs = operation.getCombinableStreamOperations
          val dependentRDDs = (for (dependentExecutionGraph <- dependentExecutionGraphs) 
            yield new SparkDAGBuilder(ctx, executionName, executionConfig).build(dependentExecutionGraph).asInstanceOf[RDD[Product2[_, _]]]).toList
          
          val mergedWithCurrentRDDs = (rdd:: dependentRDDs).asInstanceOf[List[RDD[Product2[_, _]]]]
         
          val func = operation.getStreamOperationFunction.asInstanceOf[SerFunction[java.util.stream.Stream[java.util.stream.Stream[_]], java.util.stream.Stream[_]]]
          val combineStageRDD = new ShuffledStreamsCombiningRDD(mergedWithCurrentRDDs, func, new HashPartitioner(partitionCount)) // TODO figure out partitioner
          rdd = combineStageRDD
        } else {
          val nextFunction = operation.getStreamOperationFunction.asInstanceOf[SerFunction[java.util.stream.Stream[Entry[Object,_ <: java.util.Iterator[_]]], java.util.stream.Stream[_]]]
          val nextStageRDD = new ShuffledStreamRDD(rdd.asInstanceOf[RDD[Product2[Object, Object]]], nextFunction, new HashPartitioner(partitionCount)) // TODO figure out partitioner
          rdd = nextStageRDD
        }
      }
    }
    rdd
  }
  
  private def getPath(pipelineName:String, executionConfig:Properties, executionName:String):String = {
    val sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + pipelineName);
    Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + pipelineName +  "' property can not be found in " + 
        executionName + ".cfg configuration file.");
    
    val sourceSupplier = SourceSupplier.create(sourceProperty, null);
    val sources = sourceSupplier.get().asInstanceOf[Array[Object]];
    Assert.notEmpty(sources, "sources must not be null or empty");
    
    if (sources(0).isInstanceOf[URI]){
      val uriSources = Arrays.copyOf(sources, sources.length, classOf[Array[URI]]);
      val path = uriSources.map(uri => uri.toString()).reduce((a,b) => a + "," +b)
      return path
    }
    else {
      throw new IllegalStateException("SourceSuppliers other then URISourceSupplier are not supported at the moment");
    }
  }
}