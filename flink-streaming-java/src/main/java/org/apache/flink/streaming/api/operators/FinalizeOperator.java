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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FinalizeFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing a {@link org.apache.flink.api.dag.FinalizeTransformation}.
 */
@Internal
public class FinalizeOperator<InputT>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<InputT, Void>, BoundedOneInput {

	private static final long serialVersionUID = 1L;

	private final FinalizeFunction finalizeFunction;

	public FinalizeOperator(FinalizeFunction finalizeFunction) {
		this.finalizeFunction = finalizeFunction;
	}

	@Override
	public void endInput() throws Exception {
		finalizeFunction.finalizeOutput();
	}

	@Override
	public void processElement(StreamRecord<InputT> element) throws Exception {
		// we ignore any input
	}
}
