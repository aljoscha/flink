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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.functions.FinalizeFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.dag.Commit;
import org.apache.flink.streaming.api.dag.Finalize;
import org.apache.flink.streaming.api.dag.Map;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.UnionTransformation;

/**
 * Example for the new topology-based Sink API.
 */
public class SinkExample {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> input = env.fromElements(
				"guowei",
				"klou",
				"yun",
				"aljoscha");

		// see how nice it is to use the sink for a user
		input.sink(new MyStupidSink());

		env.execute();
	}

	/**
	 * Example sink that just prints commits when committing.
	 */
	public static class MyStupidSink extends Sink<String> {
		@Override
		public Transformation<?> apply(Context context, Transformation<String> input) {

			Transformation<String> mapped = input.apply(context, Map.of(new MySinkMapper("1")));

			Transformation<Void> committed1 = mapped.apply(
					context,
					Commit.of(new MyCommitter("1")));
			Transformation<Void> committed2 = mapped.apply(
					context,
					Commit.of(new MyCommitter("2")));

			Transformation<String> mapped2 = mapped.apply(context, Map.of(new MySinkMapper("2")));
			Transformation<String> mapped3 = mapped.apply(context, Map.of(new MySinkMapper("3")));

			Transformation<String> union = UnionTransformation.of(mapped2, mapped3);
			Transformation<Void> committedUnion = union.apply(
					context,
					Commit.of(new MyCommitter("union")));

			Transformation<Void> unionAll = UnionTransformation.of(
					committed1,
					committed2,
					committedUnion);

			return unionAll.apply(context, Finalize.of(new MyFinalizer("all")));
		}

		public static class MySinkMapper implements MapFunction<String, String> {

			private final String id;

			public MySinkMapper(String id) {
				this.id = id;
			}

			@Override
			public String map(String value) {
				return id + ": Hello " + value;
			}
		}

		public static class MyCommitter implements CommitFunction<String> {
			private final String id;

			public MyCommitter(String id) {
				this.id = id;
			}

			@Override
			public void commit(String commit) {
				System.out.println(id + ": Committing " + commit);
			}
		}

		public static class MyFinalizer implements FinalizeFunction {
			private final String id;

			public MyFinalizer(String id) {
				this.id = id;
			}

			@Override
			public void finalizeOutput() {
				System.out.println(id + ": Finalizing");
			}
		}
	}

}
