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
package dstream.support;

import dstream.utils.Tuples.Tuple;
import dstream.utils.Tuples.Tuple6;

public interface Tuple7<A,B,C,D,E,F,G> extends Tuple6<A, B, C, D, E, F> {
	@SuppressWarnings("unchecked")
	public static <A,B,C,D,E,F,G> Tuple7<A,B,C,D,E,F,G> tuple7(A _1, B _2, C _3, D _4, E _5, F _6, G _7) {
		return (Tuple7<A,B,C,D,E,F,G>) new Tuple(_1, _2, _3, _4, _5, _6, _7);
	}
	
	default A _7() {
		return ((Tuple)this).get(6);
	}
}
