import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { executionKeys } from "./executions";
import type { EventView, ExecutionDetail } from "@/types/execution";

/**
 * Live event stream for a single execution over Server-Sent Events.
 *
 * <h2>What it does</h2>
 *
 * <p>Opens a native {@link EventSource} to {@code /api/v1/executions/{id}/stream}. The server
 * sends the full history as a burst of {@code event} messages on connect (the snapshot), a
 * {@code ready} marker, then one {@code event} per row as it is appended. Each event is folded
 * into the TanStack Query cache for this execution's detail, so the DAG - which renders purely
 * from that cached history - recolors itself with no imperative redraw code.</p>
 *
 * <h2>Why patch the cache instead of refetching</h2>
 *
 * <p>The naive approach is to invalidate the detail query on every SSE message and let it
 * refetch. That works but defeats the point: it turns a push into a poll, doubling the request
 * count and adding a round trip of latency to every event. Instead we mutate the cached
 * ExecutionDetail directly - appending the new event to history, deduplicated by sequence
 * number - so the UI updates from the pushed data alone. A refetch happens only once, lazily,
 * if the cache is somehow empty when the first event arrives.</p>
 *
 * <h2>Reconnection</h2>
 *
 * <p>EventSource reconnects automatically on transport errors, so the hook does not implement
 * its own retry. It does track a connection status for a live/offline indicator, and it closes
 * the stream cleanly when the execution id changes or the component unmounts, preventing a leak
 * of half-open connections as the user navigates between executions.</p>
 */
export type StreamStatus = "connecting" | "open" | "closed";

export function useExecutionStream(execId: string | null): {
  status: StreamStatus;
  lastSeq: number | null;
} {
  const queryClient = useQueryClient();
  const [status, setStatus] = useState<StreamStatus>("closed");
  const [lastSeq, setLastSeq] = useState<number | null>(null);
  // Guard against applying an event we already have after a reconnect replays the snapshot.
  const seenSeqs = useRef<Set<number>>(new Set());

  useEffect(() => {
    if (!execId) {
      setStatus("closed");
      return;
    }

    seenSeqs.current = new Set();
    setStatus("connecting");
    const source = new EventSource(`/api/v1/executions/${execId}/stream`);

    const applyEvent = (view: EventView) => {
      if (seenSeqs.current.has(view.sequenceNumber)) return;
      seenSeqs.current.add(view.sequenceNumber);
      setLastSeq((prev) =>
        prev === null || view.sequenceNumber > prev ? view.sequenceNumber : prev,
      );

      queryClient.setQueryData<ExecutionDetail>(
        executionKeys.detail(execId),
        (prev) => {
          if (!prev) {
            // No snapshot in cache yet (user opened straight into the stream). Trigger one
            // lazy fetch; subsequent events patch normally.
            queryClient.invalidateQueries({ queryKey: executionKeys.detail(execId) });
            return prev;
          }
          if (prev.history.some((e) => e.sequenceNumber === view.sequenceNumber)) {
            return prev;
          }
          const history = [...prev.history, view].sort(
            (a, b) => a.sequenceNumber - b.sequenceNumber,
          );
          return { ...prev, history };
        },
      );
    };

    source.addEventListener("open", () => setStatus("open"));

    source.addEventListener("event", (e: MessageEvent) => {
      try {
        applyEvent(JSON.parse(e.data) as EventView);
      } catch {
        /* ignore a malformed frame; the next one or a reconnect recovers */
      }
    });

    source.addEventListener("ready", () => setStatus("open"));

    source.onerror = () => {
      // EventSource will auto-reconnect; reflect the transient drop in the indicator.
      setStatus("connecting");
    };

    return () => {
      source.close();
      setStatus("closed");
    };
  }, [execId, queryClient]);

  return { status, lastSeq };
}
