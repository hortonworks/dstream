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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * 
 */
@SuppressWarnings("unchecked")
public abstract class TypeAwareWritable<T> implements NewWritable<T> {
	
    protected T value;
	
	private byte valueType = NULL;
	
	private final ValueEncoder valueEncoder = new ValueEncoder();

	/**
	 * 
	 */
	public void setValue(T value) {
		this.value = value;
		this.determineValueType(value);
		this.valueType = this.valueEncoder.valueType;
	}
	
	/**
	 * 
	 */
	public T getValue() {
		return this.value;
	}
	
	
	
	/**
	 * 
	 */
	@Override
	public int hashCode() {
		int hashCode = 0;
		if (this.value != null){
			hashCode = this.value.hashCode();
		}
		return hashCode;
	}
	
	/**
	 * 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(this.valueType);
		if (this.valueType != NULL){
			try {
				out.write(this.valueEncoder.valueBytes);
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.valueType = in.readByte();
		switch (this.valueType) {
		case INTEGER:
			this.value = (T) Integer.valueOf(in.readInt());
			break;
		case LONG:
			this.value = (T) Long.valueOf(in.readLong());
			break;
		case NULL:
			this.value = null;
			break;
		case OBJECT:
			try {
				ObjectInputStream ois = new ObjectInputStream((DataInputStream) in);
				T value = (T) ois.readObject();
				this.value = (T) value;
			} catch (Exception e) {
				throw new IllegalStateException(
						"Failed to deserialize value", e);
			}
			break;
		default:
			throw new IllegalStateException(
					"Unsupported or unrecognized value type: "
							+ this.valueType);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		if (this.value == null){
			return null;
		} else {
			return this.value.toString();
		}
	}
	
	/**
	 * NOTE: The below code is temporary and both conversion and ser/deser will be exposed through externally 
	 * configurable framework!
	 * 
	 * @param value
	 * @return
	 */
	private void determineValueType(Object value){
		if (value instanceof Integer){
			this.valueEncoder.valueType = INTEGER;
			this.valueEncoder.valueBytes = ByteBuffer.allocate(4).putInt((Integer)value).array();
		} 
		else if (value instanceof Long) {
			this.valueEncoder.valueType = LONG;
			this.valueEncoder.valueBytes = ByteBuffer.allocate(8).putLong((Long)value).array();
		}
		else if (value == null){
			this.valueEncoder.valueType = NULL;
		}
		else {
			ObjectOutputStream oos = null;
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				oos = new ObjectOutputStream(bos);
				oos.writeObject(value);
				this.valueEncoder.valueType = OBJECT;
				this.valueEncoder.valueBytes = bos.toByteArray();
			} catch (Exception e) {
				throw new IllegalStateException("Failed to serialize value: " + value, e);
			} finally {
				try {
					if (oos != null){
						oos.close();
					}
				} catch (Exception e2) {/*ignore*/}
			}
		}
	}
	
	private final static byte INTEGER = -128;
	private final static byte STRING = -127;
	private final static byte LONG = -126;
	private final static byte OBJECT = -125;
	private final static byte NULL = -124;
	
	/**
	 */
	private class ValueEncoder {
		private byte valueType;
		private byte[] valueBytes;
	}
}
