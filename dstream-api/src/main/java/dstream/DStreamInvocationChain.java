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
package dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A sequence of {@link DStreamInvocation}s
 */
final class DStreamInvocationChain {

	private final List<DStreamInvocation> invocations;

	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	private final Class<?> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceIdentifier
	 * @param streamType
	 */
	protected DStreamInvocationChain(Class<?> sourceElementType, String sourceIdentifier, Class<?> streamType){
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.invocations = new ArrayList<>();
		this.streamType = streamType;
	}
	
	@Override
	public String toString(){
		return "SOURCE_ID:" + sourceIdentifier + 
				", INVOCATIONS:" + invocations;
	}
	
	/**
	 * 
	 * @return
	 */
	public Class<?> getStreamType() {
		return this.streamType;
	}

	/**
	 * 
	 * @return
	 */
	public List<DStreamInvocation> getInvocations() {
		return Collections.unmodifiableList(this.invocations);
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return this.sourceElementType;
	}

	/**
	 * 
	 * @return
	 */
	public String getSourceIdentifier() {
		return this.sourceIdentifier;
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addInvocation(DStreamInvocation invocation){
		this.invocations.add(invocation);
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addAllInvocations(List<DStreamInvocation> invocations){
		this.invocations.addAll(invocations);
	}
	
	/**
	 * 
	 * @return
	 */
	protected DStreamInvocation getLastInvocation(){
		if (this.invocations.size() == 0){
			return null;
		}
		return this.invocations.get(this.invocations.size()-1);
	}
	
	/**
	 * 
	 */
	static class DStreamInvocation {
		
		private final Method method;

		private final Object[] arguments;
		
		private Object supplementaryOperation;

		/**
		 * Constructs this invocation.
		 * 
		 * @param method - {@link Method} invoked on {@link DStream} operation.
		 * @param arguments - arguments of the {@link Method} invoked on {@link DStream} operation.
		 */
		DStreamInvocation(Method method, Object... arguments){
			this.method = method;
			this.arguments = arguments;
		}
		
		/**
		 * Returns {@link Method} invoked on {@link DStream} operation.
		 * @return {@link Method} invoked on {@link DStream} operation.
		 */
		public Method getMethod() {
			return method;
		}

		/**
		 * Returns arguments of the {@link Method} invoked on {@link DStream} operation.
		 * @return arguments of the {@link Method} invoked on {@link DStream} operation.
		 */
		public Object[] getArguments() {
			return arguments;
		}
		
		/**
		 * 
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public <T> T getSupplementaryOperation(){
			return (T) this.supplementaryOperation;
		}
		
		/**
		 * 
		 * @param operation
		 */
		protected void setSupplementaryOperation(Object supplementaryOperation) {
			this.supplementaryOperation = supplementaryOperation;
		}
		
		/**
		 * 
		 */
		@Override
		public String toString(){
			return "{OP:" + this.method.getName() + ", ARG:" + 
					Arrays.asList(this.arguments) + ", SUP:" + this.supplementaryOperation + "}";
		}
	}
}
