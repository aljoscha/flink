package org.apache.flink.api.dag;

import org.apache.flink.api.common.functions.FinalizeFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * A transformation that can finalize something when a job finishes.
 */
public class FinalizeTransformation<InputT> extends Transformation<Void> {

	private final Transformation<InputT> input;

	private final FinalizeFunction finalizeFunction;

	/**
	 * Creates a new {@code FinalizeTransformation} that has the given input and uses the given
	 * {@link FinalizeFunction} for finalizing at the "end" of the job.
	 */
	public FinalizeTransformation(
			Transformation<InputT> input,
			FinalizeFunction finalizeFunction) {
		super("Finalize", (TypeInformation) TypeExtractor.getForClass(Object.class), 1);
		this.input = input;
		this.finalizeFunction = finalizeFunction;
	}

	/**
	 * Returns the input {@code Transformation}.
	 */
	public Transformation<InputT> getInput() {
		return input;
	}

	/**
	 * Returns the {@link FinalizeFunction} in this transformation.
	 */
	public FinalizeFunction getFinalizeFunction() {
		return finalizeFunction;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}
