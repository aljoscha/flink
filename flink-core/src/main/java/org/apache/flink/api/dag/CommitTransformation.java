package org.apache.flink.api.dag;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * A transformation that collects input "commits" and commits them at the "end" of the job, what
 * ever that means. I'm really just spit balling here and this is an example of a new custom {@link
 * Transformation} that we could introduce.
 */
public class CommitTransformation<CommittableT> extends Transformation<Void> {

	private final Transformation<CommittableT> input;

	private final CommitFunction<CommittableT> commitFunction;

	/**
	 * Creates a new {@code CommitTransformation} that has the given input and uses the given {@link
	 * CommitFunction} for committing at the "end" of the job.
	 */
	public CommitTransformation(
			Transformation<CommittableT> input,
			CommitFunction<CommittableT> commitFunction) {
		super("Commit", (TypeInformation) TypeExtractor.getForClass(Object.class), 1);
		this.input = input;
		this.commitFunction = commitFunction;
	}

	/**
	 * Returns the input {@code Transformation}.
	 */
	public Transformation<CommittableT> getInput() {
		return input;
	}

	/**
	 * Returns the {@link CommitFunction} in this transformation.
	 */
	public CommitFunction<CommittableT> getCommitFunction() {
		return commitFunction;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}
