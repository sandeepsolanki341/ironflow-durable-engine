import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { api } from "./client";
import type { TelemetrySample } from "@/types/execution";

/**
 * Polls the telemetry endpoint and keeps a rolling client-side window of samples for the live
 * charts.
 *
 * <p>The server returns one instantaneous sample per call; the time series lives here, in the
 * browser, capped at {@code windowSize} points. Keeping the history client-side means the
 * backend holds no per-dashboard state and its memory does not grow with how long a dashboard
 * has been left open - the cost of a long-running chart is bounded by the window, on the client
 * that is actually looking at it.</p>
 *
 * @param windowSize max points to retain (default 60 — one minute at a 1s interval)
 * @param intervalMs poll cadence
 */
export function useTelemetry(windowSize = 60, intervalMs = 2000) {
  const [samples, setSamples] = useState<TelemetrySample[]>([]);
  const seq = useRef(0);

  const query = useQuery({
    queryKey: ["telemetry", "sample"],
    queryFn: () => api.get<TelemetrySample>("/telemetry"),
    refetchInterval: intervalMs,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (!query.data) return;
    setSamples((prev) => {
      // Dedup identical timestamps (a cached refetch could repeat one).
      if (prev.length && prev[prev.length - 1].timestamp === query.data!.timestamp) {
        return prev;
      }
      seq.current += 1;
      const next = [...prev, query.data!];
      return next.length > windowSize ? next.slice(next.length - windowSize) : next;
    });
  }, [query.data, windowSize]);

  return { samples, isLoading: query.isLoading, isError: query.isError };
}
