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

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToSequenceFileRDDFunctions
import io.dstream.AbstractDStreamExecutionDelegate
import io.dstream.DStreamExecutionGraph
import io.dstream.hadoop.KeyWritable
import io.dstream.hadoop.ValueWritable
import io.dstream.hadoop.SequenceFileOutputStreamsBuilder
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import io.dstream.DStreamConstants
import io.dstream.utils.Assert

/**
 * @author ozhurakousky
 */
class SparkDStreamExecutionDelegate extends AbstractDStreamExecutionDelegate with Logging {
  
  private var sc:SparkContext = _
  
  private var fs:FileSystem = _
 
  /**
   * 
   */
  override def getCloseHandler():Runnable = {
    return new Runnable() {
      override def run() {
        logInfo("STOPPING SparkContext")
        sc.stop();
      }
    };  
  }
  
  /**
   * 
   */
  override protected def doExecute(executionName:String, executionConfig:Properties, executionPipelines:DStreamExecutionGraph*)
      :java.util.List[java.util.stream.Stream[java.util.stream.Stream[_]]] = {
    Assert.notEmpty(executionPipelines, "'executionPipelines' must not be null or empty")
    Assert.isTrue(executionPipelines.length < 2, "Multiple 'executionPipelines' are not supported at the moment");
    
    logInfo("Executing '"  + executionName + "'")
    this.initialize(executionName)
   
    val executionGraph = executionPipelines(0)
    val pipelineName = executionGraph.getName
   
    val outputPath = this.determineOutputPath(executionName, executionConfig)
    this.fs.delete(new Path(outputPath), true) //TODO make this configurable 
    
    
    val dagBuilder = new SparkDAGBuilder(this.sc, executionName, executionConfig)
    val dagRDD = dagBuilder.build(executionGraph)
    
    this.realizeRDD(dagRDD, outputPath)
  
    val sb =  new SequenceFileOutputStreamsBuilder[Any](this.fs, outputPath, this.sc.hadoopConfiguration);
    val resultStreams = sb.build()
    
    val resultStream = SparkAdapters.toResultStream[Any](sb.build())
    seqAsJavaList(List(resultStream).asInstanceOf[List[java.util.stream.Stream[java.util.stream.Stream[_]]]])
  }
  
  /**
   * 
   */
  private def determineOutputPath(executionName:String, executionConfig:Properties):String = {
    val outputPath = if (executionConfig.containsKey(DStreamConstants.OUTPUT)) {
        executionConfig.getProperty(DStreamConstants.OUTPUT)
      } else {
        executionName + "/out";
      }
    
    outputPath
  }
  
  /**
   * 
   */
  private def initialize(executionName:String) = {
    logInfo("STARTING SparkContext")
    val conf = new SparkConf().setAppName(executionName)
    if (!conf.contains("spark.master")){
      logDebug("Defaulting to 'local' -spark.master- since it was not defined in Spark configuration")
      conf.setMaster("local")
    }
    this.sc = new SparkContext(conf)
    this.fs = FileSystem.get(sc.hadoopConfiguration)
  }
  
  /**
   * 
   */
  private def realizeRDD(assembledRDD:RDD[_], outputPath:String) = {
    assembledRDD
      .map[Tuple2[KeyWritable, ValueWritable[Any]]](value => {
        val kw = new KeyWritable
        val vw = new ValueWritable[Any]
        if (value.isInstanceOf[Tuple2[_,_]]){
          val t2 = value.asInstanceOf[Tuple2[_,_]]
          kw.setValue(t2._1)
          vw.setValue(t2._2)
        }
        else {
          vw.setValue(value)
        }
        (kw, vw)
      }).saveAsSequenceFile(outputPath)
  }
}