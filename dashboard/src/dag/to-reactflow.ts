import type { Edge, Node } from "reactflow";
import { MarkerType } from "reactflow";
import type { DagModel } from "./types";
import { layoutDag } from "./layout";

// Maps our framework-independent model onto React Flow's Node/Edge shapes. All visual state
// travels in node.data so the custom node component can style itself; edges carry kind +
// animated so compensation edges render amber and flow in reverse.

export function toReactFlow(model: DagModel): { nodes: Node[]; edges: Edge[] } {
  const positioned = layoutDag(model);

  const nodes: Node[] = positioned.map((n) => ({
    id: n.id,
    type: "workflowNode",
    position: { x: n.x, y: n.y },
    data: {
      label: n.label,
      state: n.state,
      kind: n.kind,
      failure: n.failure,
      attempts: n.attempts,
      maxAttempts: n.maxAttempts,
    },
  }));

  const edges: Edge[] = model.edges.map((e) => {
    const isComp = e.kind === "compensation";
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      animated: e.animated,
      // Compensation edges are amber and point from the forward node down to its undo, so the
      // animation visibly flows in the reverse direction of the forward arrows above.
      style: {
        stroke: isComp ? "#f59e0b" : "#94a3b8",
        strokeWidth: isComp ? 2.5 : 1.5,
        strokeDasharray: isComp ? "6 3" : undefined,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: isComp ? "#f59e0b" : "#94a3b8",
      },
    };
  });

  return { nodes, edges };
}
