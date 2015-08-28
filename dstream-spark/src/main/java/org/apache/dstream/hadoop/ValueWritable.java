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
package org.apache.dstream.hadoop;



/**
 * This class represents a universal Writable with the goal of recognizing 
 * and properly writing/reading multiple types of data.
 * More documentation to follow as its design progresses, but at the moment the mechanism is simple:
 * 1. Prepend each value with a byte representing its type
 * 2. Write value data as byte[]
 * For fixed formats such as Int, Long etc., its standard byte representation is used (e.g., Int = 4 bytes)
 * For Objects (variable length) object serialization is used with custom se/deser to be exposed for 
 * customization and optimization
 * 
 * This class is not public nor it is meant/designed as thread-safe.
 */
public class ValueWritable<T> extends TypeAwareWritable<T>  {

}
