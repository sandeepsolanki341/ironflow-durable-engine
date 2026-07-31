package io.ironflow.persistence.model;

/** Lifecycle of a queued task. */
public enum TaskStatus {
    PENDING, LEASED, COMPLETED, FAILED, CANCELLED;

    /**
     * @return {@code true} if this status occupies the hot-path partial indexes.
     *         Open tasks are the only ones the dispatcher and reaper can see; closed
     *         tasks leave the indexes entirely, which is what lets one table serve as
     *         a queue indefinitely without an archive.
     */
    public boolean isOpen() {
        return this == PENDING || this == LEASED;
    }
}
