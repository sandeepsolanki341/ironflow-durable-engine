package io.ironflow.replay;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.helpers.AbstractLogger;

import java.util.function.BooleanSupplier;

/**
 * Delegating logger that drops output while replaying.
 *
 * <p>Without this, a workflow that logs once per step logs that line again on every
 * subsequent decision task - a workflow with fifty steps produces a quadratic pile of
 * duplicate log lines, and an operator reading them cannot tell a genuine retry from a
 * routine replay.</p>
 */
final class ReplayAwareLogger extends AbstractLogger {

    private final Logger delegate;
    private final BooleanSupplier replaying;

    ReplayAwareLogger(Logger delegate, BooleanSupplier replaying) {
        this.delegate = delegate;
        this.replaying = replaying;
    }

    @Override
    protected void handleNormalizedLoggingCall(org.slf4j.event.Level level, Marker marker,
                                               String messagePattern, Object[] arguments,
                                               Throwable throwable) {
        if (replaying.getAsBoolean()) {
            return;
        }
        switch (level) {
            case TRACE -> delegate.trace(marker, messagePattern, arguments);
            case DEBUG -> delegate.debug(marker, messagePattern, arguments);
            case INFO  -> delegate.info(marker, messagePattern, arguments);
            case WARN  -> delegate.warn(marker, messagePattern, arguments);
            case ERROR -> delegate.error(marker, messagePattern, arguments);
        }
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return ReplayAwareLogger.class.getName();
    }

    @Override public boolean isTraceEnabled() { return delegate.isTraceEnabled(); }
    @Override public boolean isTraceEnabled(Marker m) { return delegate.isTraceEnabled(m); }
    @Override public boolean isDebugEnabled() { return delegate.isDebugEnabled(); }
    @Override public boolean isDebugEnabled(Marker m) { return delegate.isDebugEnabled(m); }
    @Override public boolean isInfoEnabled() { return delegate.isInfoEnabled(); }
    @Override public boolean isInfoEnabled(Marker m) { return delegate.isInfoEnabled(m); }
    @Override public boolean isWarnEnabled() { return delegate.isWarnEnabled(); }
    @Override public boolean isWarnEnabled(Marker m) { return delegate.isWarnEnabled(m); }
    @Override public boolean isErrorEnabled() { return delegate.isErrorEnabled(); }
    @Override public boolean isErrorEnabled(Marker m) { return delegate.isErrorEnabled(m); }
}
