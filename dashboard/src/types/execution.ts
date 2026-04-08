// These types mirror the Java DTOs one-for-one. Keeping them in a single file makes the
// contract auditable against the backend: ExecutionStatus <-> ExecutionStatus enum,
// ExecutionSummary <-> ExecutionSummaryResponse, ExecutionDetail <-> ExecutionDetailResponse.

/**
 * Every lifecycle state the backend's ExecutionStatus enum can emit. The three non-terminal
 * states (RUNNING, COMPENSATING, DIVERGENT) have a null endTime; the rest are terminal.
 * Kept as a const array (not just a union) so the UI can iterate the full set for filters.
 */
export const EXECUTION_STATUSES = [
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "COMPENSATING",
  "FAILED_COMPENSATED",
  "DIVERGENT",
  "CANCELLED",
  "TIMED_OUT",
] as const;

export type ExecutionStatus = (typeof EXECUTION_STATUSES)[number];

/** Non-terminal states carry no endTime; used to compute a live, ticking duration. */
export const NON_TERMINAL_STATUSES: ReadonlySet<ExecutionStatus> = new Set([
  "RUNNING",
  "COMPENSATING",
  "DIVERGENT",
]);

/** A row in the list — mirrors ExecutionSummaryResponse. */
export interface ExecutionSummary {
  executionId: string;
  workflowType: string;
  businessKey: string | null;
  status: ExecutionStatus;
  startTime: string; // ISO-8601 instant
  endTime: string | null;
}

/** One history event — mirrors ExecutionDetailResponse.EventView. */
export interface EventView {
  sequenceNumber: number;
  eventType: string;
  payload: unknown;
  createdAt: string;
}

/** Full detail — mirrors ExecutionDetailResponse. */
export interface ExecutionDetail {
  executionId: string;
  workflowType: string;
  businessKey: string | null;
  status: ExecutionStatus;
  currentVersion: number;
  startTime: string;
  endTime: string | null;
  result: unknown | null;
  failure: string | null;
  divergenceDetail: string | null;
  history: EventView[];
}

/** Generic page envelope — mirrors PageResponse<T>. */
export interface PageResponse<T> {
  items: T[];
  page: number;
  size: number;
  totalItems: number;
  totalPages: number;
}

/** Query parameters accepted by GET /api/v1/executions. */
export interface ListExecutionsParams {
  status?: ExecutionStatus | null;
  businessKey?: string | null;
  page?: number;
  size?: number;
}

/** One telemetry reading — mirrors the backend TelemetrySample record. */
export interface TelemetrySample {
  timestamp: string;
  queueDepth: number;
  leasedTasks: number;
  dispatchLatencyMs: number | null;
  activeVirtualThreads: number | null;
}
