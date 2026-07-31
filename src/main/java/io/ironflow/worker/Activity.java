package io.ironflow.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as an activity implementation.
 *
 * <p><b>Activities must be idempotent.</b> Activity execution is at-least-once: a worker
 * that dies mid-activity has its task reclaimed and re-executed, so the side effect can and
 * will happen twice. Use {@link ActivityContext#attempt()} to detect a retry and skip work
 * that has already been done, and prefer idempotency keys on downstream calls.</p>
 *
 * <p>The accepted signatures are {@code (Input)} or {@code (Input, ActivityContext)}.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Activity {
    /** The registered type name, matching what workflows pass to {@code ctx.activity}. */
    String value();
}
