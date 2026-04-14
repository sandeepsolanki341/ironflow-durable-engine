// The DAG model, decoupled from React Flow. The parser produces these; a thin adapter maps
// them to reactflow Node/Edge. Keeping our own types means the parser is testable without
// pulling in the whole rendering library, and a future renderer swap touches only the adapter.

import type { ExecutionStatus } from "@/types/execution";

/**
 * The visual state of a single node, driving its color/animation. Derived purely from the
 * event history — never guessed. The four states the prompt calls out map here directly:
 *  - COMPLETED  -> green + checkmark
 *  - ACTIVE     -> blue pulsing border (running activity OR a live timer/sleep)
 *  - FAILED     -> red
 *  - COMPENSATING / COMPENSATED -> amber/orange (the reverse-flow rollback)
 */
export type NodeState =
  | "PENDING" // scheduled, no outcome yet (rare in a settled history; shown neutral)
  | "ACTIVE" // running activity, or a started-but-unfired timer (sleeping)
  | "COMPLETED" // activity completed / timer fired
  | "FAILED" // activity failed after exhausting retries
  | "COMPENSATING" // a compensation for this node is currently running
  | "COMPENSATED"; // a compensation for this node has completed (undo done)

/** What kind of step this node represents — changes the icon and label treatment. */
export type NodeKind = "start" | "activity" | "timer" | "compensation" | "end";

export interface DagNode {
  /** Stable id: the scheduling event's sequence number, as a string. */
  id: string;
  kind: NodeKind;
  /** Human label — the activity type, "Sleep 3d", "Refund (undo)", etc. */
  label: string;
  state: NodeState;
  /** The activity/compensation type name, when applicable. */
  activityType?: string;
  /** Failure text, present only when state is FAILED. */
  failure?: string;
  /** For a compensation node: the forward node id it undoes. */
  compensatesNodeId?: string;
  /** Attempt info for activities, surfaced in the node tooltip/subtitle. */
  attempts?: number;
  maxAttempts?: number;
  /** Sequence number of the originating event, for deterministic ordering/layout. */
  seq: number;
}

export type EdgeKind = "forward" | "compensation";

export interface DagEdge {
  id: string;
  source: string;
  target: string;
  kind: EdgeKind;
  /** True while this edge should show flowing animation (active forward or active undo). */
  animated: boolean;
}

export interface DagModel {
  nodes: DagNode[];
  edges: DagEdge[];
  /** Overall execution status, passed through for the header/legend. */
  status: ExecutionStatus | null;
}
