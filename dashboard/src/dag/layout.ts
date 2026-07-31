// Deterministic layered layout. React Flow needs x/y for every node; we assign them from the
// parsed model so the graph is stable across renders (same history -> same picture). Forward
// nodes march left-to-right in one row; compensation nodes drop into a lane below the node
// they undo, so the reverse-flow rollback reads as "unwinding underneath".

import type { DagModel, DagNode } from "./types";

export interface PositionedNode extends DagNode {
  x: number;
  y: number;
}

const COL_WIDTH = 200;
const ROW_FORWARD = 40;
const ROW_COMPENSATION = 220;

export function layoutDag(model: DagModel): PositionedNode[] {
  // Order forward nodes by seq; assign columns in that order.
  const forward = model.nodes
    .filter((n) => n.kind !== "compensation")
    .sort((a, b) => a.seq - b.seq);

  const colOf = new Map<string, number>();
  forward.forEach((n, i) => colOf.set(n.id, i));

  const positioned: PositionedNode[] = [];

  for (const n of forward) {
    positioned.push({ ...n, x: (colOf.get(n.id) ?? 0) * COL_WIDTH, y: ROW_FORWARD });
  }

  // Compensation nodes sit under the forward node they compensate. If two share a column
  // (shouldn't happen in a simple saga), stack them further down.
  const usedComp = new Map<number, number>();
  for (const n of model.nodes.filter((m) => m.kind === "compensation")) {
    const forwardCol = n.compensatesNodeId ? colOf.get(n.compensatesNodeId) : undefined;
    const col = forwardCol ?? 0;
    const stack = usedComp.get(col) ?? 0;
    usedComp.set(col, stack + 1);
    positioned.push({
      ...n,
      x: col * COL_WIDTH,
      y: ROW_COMPENSATION + stack * 90,
    });
  }

  return positioned;
}
