package io.ironflow.sdk;

/**
 * User-implemented workflow definition.
 *
 * <h2>The determinism contract</h2>
 *
 * <p>{@link #run} is invoked <em>repeatedly</em> - once per decision task, each time
 * against a longer history. It must issue the same sequence of SDK calls every time, or
 * the engine cannot match recorded results to the calls that produced them.</p>
 *
 * <p>In practice this means: no {@code Instant.now()}, no {@code new Random()}, no
 * {@code UUID.randomUUID()}, no direct I/O, no reading mutable static state, and no
 * iteration over collections with nondeterministic order ({@code HashMap} and
 * {@code HashSet} are the usual culprits). Use {@link WorkflowContext} for all of it.</p>
 *
 * <p>Ordinary control flow is fine and expected - loops, conditionals, try/finally, helper
 * methods, exceptions. That is the point of this design: durable execution without async
 * plumbing.</p>
 *
 * @param <I> input type, deserialized from the execution's stored input
 * @param <O> output type, serialized into the execution's result
 */
public interface Workflow<I, O> {

    /** @return the registered type name; must be unique across all workflow beans. */
    String type();

    /** @return the input class, for deserialization. Erasure means we cannot infer it. */
    Class<I> inputType();

    /**
     * The workflow body.
     *
     * @throws Exception any failure. An {@link ActivityFailure} the workflow chooses not
     *         to catch fails the workflow; anything else is a workflow-level failure.
     */
    O run(I input, WorkflowContext ctx) throws Exception;
}
