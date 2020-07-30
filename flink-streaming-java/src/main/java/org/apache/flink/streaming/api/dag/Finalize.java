package org.apache.flink.streaming.api.dag;

import org.apache.flink.api.common.functions.FinalizeFunction;
import org.apache.flink.api.dag.FinalizeTransformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.dag.TransformationApply;

/**
 * Applies a {@link FinalizeFunction} on the input {@link Transformation}.
 *
 * <p>TODO: This shouldn't be in flink-streaming-java but I'm keeping them all here together for
 * the POC.
 */
public class Finalize<InputT> extends TransformationApply<Transformation<InputT>, Transformation<Void>> {

	/**
	 * Creates a new {@link Finalize} from the given {@link org.apache.flink.api.common.functions.FinalizeFunction}.
	 */
	public static <InputT> Finalize<InputT> of(FinalizeFunction finalizeFunction) {
		return new Finalize<>(finalizeFunction);
	}

	@Override
	public Transformation<Void> apply(Context context, Transformation<InputT> input) {
		FinalizeFunction cleanedFinalizeFunction = context.clean(finalizeFunction);

		return new FinalizeTransformation<>(input, cleanedFinalizeFunction);
	}

	private final FinalizeFunction finalizeFunction;

	Finalize(FinalizeFunction finalizeFunction) {
		this.finalizeFunction = finalizeFunction;
	}
}
