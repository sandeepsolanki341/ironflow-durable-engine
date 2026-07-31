package io.ironflow.api;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Purges expired buffered signals.
 *
 * <p>{@code wf_pending_signals} holds signals that arrived before their execution existed.
 * Most are drained within seconds by {@code WorkflowService.start()}. The ones that are not
 * were addressed to a business key that never got created - a typo, a cancelled upstream
 * request, a client bug - and without a sweep they accumulate forever.</p>
 *
 * <p>This was explicitly identified as a gap when signals were designed and is implemented
 * here rather than left as a note, because an unbounded table with no owner is exactly the
 * kind of thing that is discovered eighteen months later at 40GB.</p>
 *
 * <p>Deliberately quiet at INFO when it finds nothing, and loud when it finds a lot: a
 * sustained nonzero purge count means callers are signalling business keys that never
 * materialise, which is a caller bug worth surfacing.</p>
 */
@Service
public class PendingSignalReaper {

    private static final Logger log = LoggerFactory.getLogger(PendingSignalReaper.class);

    /** Bounds lock hold time; a large backlog is drained over several sweeps. */
    private static final int BATCH_SIZE = 1_000;

    private static final String PURGE_SQL = """
        DELETE FROM wf_pending_signals
         WHERE id IN (
            SELECT id FROM wf_pending_signals
             WHERE expires_at < now()
             ORDER BY expires_at
             LIMIT ?
         )
        """;

    private final DSLContext dsl;

    public PendingSignalReaper(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Hourly sweep. Buffered signals have a 24-hour default lifetime, so an hourly cadence
     * bounds the overshoot to a few percent while keeping the query cost negligible.
     */
    @Scheduled(fixedDelayString = "${ironflow.signals.purge-interval-ms:3600000}",
               initialDelayString = "${ironflow.signals.purge-initial-delay-ms:60000}")
    public void purgeExpired() {
        try {
            int purged = purgeBatch();
            if (purged > 0) {
                log.warn("Purged {} expired buffered signal(s). A sustained nonzero count "
                        + "means callers are signalling business keys that are never "
                        + "created - check for a typo or a cancelled upstream flow.", purged);
            }
        } catch (Exception e) {
            // Never propagate: an exception out of a @Scheduled method leaves no
            // application-level signal, so a persistent failure would look like an idle
            // system rather than a broken sweep.
            log.error("Pending signal purge failed; will retry next interval", e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int purgeBatch() {
        return dsl.execute(PURGE_SQL, BATCH_SIZE);
    }
}
