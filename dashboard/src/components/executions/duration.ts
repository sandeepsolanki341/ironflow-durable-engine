import { NON_TERMINAL_STATUSES, type ExecutionStatus } from "@/types/execution";

/**
 * Human-readable elapsed time between a start instant and either an end instant (terminal)
 * or "now" (still live). Kept as a pure function so it is trivially unit-testable and has no
 * React dependency.
 */
export function formatDuration(
  startIso: string,
  endIso: string | null,
  status: ExecutionStatus,
  now: number = Date.now(),
): string {
  const start = new Date(startIso).getTime();
  const end =
    endIso !== null
      ? new Date(endIso).getTime()
      : NON_TERMINAL_STATUSES.has(status)
        ? now
        : start; // terminal but null endTime shouldn't happen; degrade to 0 rather than NaN

  const ms = Math.max(0, end - start);
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ${sec % 60}s`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ${min % 60}m`;
  const day = Math.floor(hr / 24);
  return `${day}d ${hr % 24}h`;
}
