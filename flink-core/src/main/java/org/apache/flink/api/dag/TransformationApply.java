/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.dag;

import org.apache.flink.annotation.Internal;

/**
 * Applies {@link Transformation Transformations} and potentially returns a new {@link
 * Transformation}.
 *
 * <p>I don't like that this is a separate interface, potentially we can integrate {@link
 * #apply(Object)} into {@link Transformation} itself, à la Beam.
 */
@Internal
public abstract class TransformationApply<InputT, OutputT> {
	public abstract OutputT apply(InputT input);
}
