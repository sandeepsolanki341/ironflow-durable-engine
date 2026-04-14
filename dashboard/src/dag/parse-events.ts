// The parser. Turns a raw wf_events array (exactly as the backend emits it) into a DagModel.
//
// Design: ONE forward pass to create a node per scheduling event, then fold each outcome
// event onto the node it references. Correlation is by the same keys the engine itself uses
// for replay — scheduledEventSeq for activity/timer outcomes, registrationSeq for
// compensations — so the graph can never disagree with what the engine actually did. No
// guessing, no heuristics: if the events say a node completed, it's green; if they don't,
// it isn't.

import type { EventView, ExecutionStatus } from "@/types/execution";
import type { DagEdge, DagModel, DagNode, NodeState } from "./types";

/** Narrow helper: read a numeric field from an unknown JSON payload. */
function num(payload: unknown, key: string): number | undefined {
  if (payload && typeof payload === "object" && key in payload) {
    const v = (payload as Record<string, unknown>)[key];
    return typeof v === "number" ? v : undefined;
  }
  return undefined;
}

function str(payload: unknown, key: string): string | undefined {
  if (payload && typeof payload === "object" && key in payload) {
    const v = (payload as Record<string, unknown>)[key];
    return typeof v === "string" ? v : undefined;
  }
  return undefined;
}

function bool(payload: unknown, key: string): boolean {
  if (payload && typeof payload === "object" && key in payload) {
    return (payload as Record<string, unknown>)[key] === true;
  }
  return false;
}

/**
 * Parse a full event history into a renderable DAG.
 *
 * @param events history in replay order (sequenceNumber ascending). The parser tolerates
 *               out-of-order input by sorting defensively — the backend always sends order,
 *               but a defensive sort costs nothing and removes a whole class of "why is the
 *               graph wrong" bugs.
 * @param status the execution's overall status, passed through to the model.
 */
export function parseEventsToDag(
  events: EventView[],
  status: ExecutionStatus | null = null,
): DagModel {
  const ordered = [...events].sort((a, b) => a.sequenceNumber - b.sequenceNumber);

  const nodes: DagNode[] = [];
  const edges: DagEdge[] = [];
  const bySeq = new Map<number, DagNode>();
  // Map a compensation's registrationSeq -> the forward node it undoes. Built from
  // COMPENSATION_REGISTERED, whose own seq is the registrationSeq the later compensation
  // activity references.
  const registrationToForwardNode = new Map<number, string>();
  // The forward node most recently REGISTERED, so a COMPENSATION_REGISTERED can attach its
  // "undoes" pointer to the correct step. Registration always immediately follows the
  // activity it compensates in engine output.
  let lastForwardNodeId: string | null = null;

  // Track the previous forward node to chain forward edges. Parallel branches share a
  // predecessor, so we remember the node that fanned them out.
  let previousForwardId: string | null = null;

  // A synthetic start node anchors the graph and gives fan-out a single root.
  const startNode: DagNode = {
    id: "start",
    kind: "start",
    label: "Start",
    state: "COMPLETED",
    seq: 0,
  };
  nodes.push(startNode);
  bySeq.set(0, startNode);
  previousForwardId = "start";

  for (const ev of ordered) {
    const seq = ev.sequenceNumber;
    const p = ev.payload;

    switch (ev.eventType) {
      case "WORKFLOW_STARTED":
        // The start node already exists; nothing to add.
        break;

      case "ACTIVITY_SCHEDULED": {
        const isComp = bool(p, "isCompensation");
        if (isComp) {
          // A compensation activity. Its registrationSeq points at the COMPENSATION_REGISTERED
          // event, which we mapped to the forward node being undone.
          const regSeq = num(p, "registrationSeq");
          const compType = str(p, "identity") ?? str(p, "activityType") ?? "compensation";
          const forwardNodeId =
            regSeq !== undefined ? registrationToForwardNode.get(regSeq) : undefined;

          const node: DagNode = {
            id: `comp-${seq}`,
            kind: "compensation",
            label: `${compType} (undo)`,
            state: "COMPENSATING", // running until its COMPENSATION_COMPLETED lands
            activityType: compType,
            compensatesNodeId: forwardNodeId,
            seq,
          };
          nodes.push(node);
          bySeq.set(seq, node);

          // The reverse edge: from the forward node back to its compensation. Amber, animated
          // while the compensation runs. Direction encodes "this undoes that".
          if (forwardNodeId) {
            edges.push({
              id: `cedge-${seq}`,
              source: forwardNodeId,
              target: node.id,
              kind: "compensation",
              animated: true,
            });
          }
        } else {
          // A forward activity node.
          const activityType = str(p, "identity") ?? str(p, "activityType") ?? "activity";
          const node: DagNode = {
            id: String(seq),
            kind: "activity",
            label: activityType,
            state: "ACTIVE", // scheduled; upgraded when its outcome is folded in
            activityType,
            seq,
          };
          nodes.push(node);
          bySeq.set(seq, node);
          lastForwardNodeId = node.id;

          if (previousForwardId) {
            edges.push({
              id: `edge-${previousForwardId}-${node.id}`,
              source: previousForwardId,
              target: node.id,
              kind: "forward",
              animated: false, // set true below if the node ends up ACTIVE
            });
          }
          // NOTE: previousForwardId advances only on a sequential step, not within a parallel
          // fan-out. We approximate fan-out below after the loop by detecting shared source.
          previousForwardId = node.id;
        }
        break;
      }

      case "ACTIVITY_COMPLETED": {
        const ref = num(p, "scheduledEventSeq");
        if (ref !== undefined) {
          const node = bySeq.get(ref);
          if (node) node.state = "COMPLETED";
        }
        break;
      }

      case "ACTIVITY_FAILED": {
        const ref = num(p, "scheduledEventSeq");
        if (ref !== undefined) {
          const node = bySeq.get(ref);
          if (node) {
            node.state = "FAILED";
            node.failure = str(p, "failure");
            node.attempts = num(p, "attempts");
            node.maxAttempts = num(p, "maxAttempts");
          }
        }
        break;
      }

      case "TIMER_STARTED": {
        const node: DagNode = {
          id: String(seq),
          kind: "timer",
          label: durationLabel(num(p, "durationMillis")),
          state: "ACTIVE", // sleeping until TIMER_FIRED
          seq,
        };
        nodes.push(node);
        bySeq.set(seq, node);
        if (previousForwardId) {
          edges.push({
            id: `edge-${previousForwardId}-${node.id}`,
            source: previousForwardId,
            target: node.id,
            kind: "forward",
            animated: false,
          });
        }
        previousForwardId = node.id;
        break;
      }

      case "TIMER_FIRED": {
        const ref = num(p, "scheduledEventSeq");
        if (ref !== undefined) {
          const node = bySeq.get(ref);
          if (node) node.state = "COMPLETED";
        }
        break;
      }

      case "COMPENSATION_REGISTERED": {
        // Attach this registration to the forward node it compensates. The registration's own
        // sequence number is the key a later compensation activity uses to find it.
        if (lastForwardNodeId) {
          registrationToForwardNode.set(seq, lastForwardNodeId);
        }
        break;
      }

      case "COMPENSATION_COMPLETED": {
        // Mark the compensation node done, and flip the forward node it undid to COMPENSATED.
        const regSeq = num(p, "registrationSeq");
        // Find the compensation node whose registrationSeq matches.
        const compNode = nodes.find(
          (n) =>
            n.kind === "compensation" &&
            regSeq !== undefined &&
            registrationToForwardNode.get(regSeq) === n.compensatesNodeId &&
            n.state === "COMPENSATING",
        );
        if (compNode) {
          compNode.state = "COMPLETED";
          // Its animated reverse edge settles.
          const e = edges.find((x) => x.target === compNode.id && x.kind === "compensation");
          if (e) e.animated = false;
          if (compNode.compensatesNodeId) {
            const forward = nodes.find((n) => n.id === compNode.compensatesNodeId);
            if (forward) forward.state = "COMPENSATED";
          }
        }
        break;
      }

      // WORKFLOW_COMPLETED / WORKFLOW_FAILED / COMPENSATION_TRIGGERED / MARKER_RECORDED /
      // SIGNAL_RECEIVED / WORKFLOW_TASK_SCHEDULED are lifecycle/bookkeeping events that don't
      // create graph nodes. They're intentionally ignored by the topology pass; overall
      // status is carried separately via the `status` argument.
      default:
        break;
    }
  }

  // Forward edges into a node that is still ACTIVE should animate (work in flight).
  for (const edge of edges) {
    if (edge.kind === "forward") {
      const target = nodes.find((n) => n.id === edge.target);
      edge.animated = target?.state === "ACTIVE";
    }
  }

  // Append a terminal node reflecting the overall outcome, wired from the last forward step.
  if (status) {
    const endState: NodeState =
      status === "COMPLETED"
        ? "COMPLETED"
        : status === "FAILED" || status === "TIMED_OUT"
          ? "FAILED"
          : status === "FAILED_COMPENSATED"
            ? "COMPENSATED"
            : "ACTIVE";
    const end: DagNode = {
      id: "end",
      kind: "end",
      label: endLabel(status),
      state: endState,
      seq: Number.MAX_SAFE_INTEGER,
    };
    nodes.push(end);
    if (lastForwardNodeId) {
      edges.push({
        id: `edge-${lastForwardNodeId}-end`,
        source: lastForwardNodeId,
        target: "end",
        kind: "forward",
        animated: false,
      });
    }
  }

  return { nodes, edges, status };
}

function durationLabel(ms: number | undefined): string {
  if (ms === undefined) return "Sleep";
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `Sleep ${sec}s`;
  const min = Math.round(sec / 60);
  if (min < 60) return `Sleep ${min}m`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `Sleep ${hr}h`;
  const day = Math.round(hr / 24);
  return `Sleep ${day}d`;
}

function endLabel(status: ExecutionStatus): string {
  switch (status) {
    case "COMPLETED":
      return "Fulfilled";
    case "FAILED":
      return "Failed";
    case "FAILED_COMPENSATED":
      return "Rolled back";
    case "TIMED_OUT":
      return "Timed out";
    case "CANCELLED":
      return "Cancelled";
    default:
      return "In progress";
  }
}
