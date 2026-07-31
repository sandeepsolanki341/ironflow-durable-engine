import { useMemo } from "react";
import ReactFlow, {
  Background,
  Controls,
  type Edge,
  type Node,
  type NodeTypes,
} from "reactflow";
import "reactflow/dist/style.css";
import { parseEventsToDag } from "@/dag/parse-events";
import { toReactFlow } from "@/dag/to-reactflow";
import type { EventView, ExecutionStatus } from "@/types/execution";
import { WorkflowNode } from "./WorkflowNode";

// Register the custom node once, at module scope. Defining nodeTypes inline in the component
// re-creates the object every render and makes React Flow warn and re-mount every node — a
// classic performance foot-gun the library explicitly calls out.
const nodeTypes: NodeTypes = { workflowNode: WorkflowNode };

interface WorkflowDagViewerProps {
  /** Raw wf_events for the execution, exactly as returned by the detail endpoint. */
  events: EventView[];
  /** Overall execution status, used for the terminal node and legend. */
  status: ExecutionStatus | null;
  height?: number | string;
}

/**
 * Live DAG view of a single workflow execution.
 *
 * <p>Pure projection of event history: the same events always render the same graph, and the
 * component holds no state of its own beyond React Flow's viewport. When the parent refetches
 * detail (the detail hook polls while an execution is live), new events flow in and nodes
 * change color in place — a completing activity goes blue→green, a failure goes red, a
 * triggered rollback lights the amber reverse edges — with no imperative update code here.</p>
 */
export function WorkflowDagViewer({
  events,
  status,
  height = 480,
}: WorkflowDagViewerProps) {
  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    const model = parseEventsToDag(events, status);
    return toReactFlow(model);
  }, [events, status]);

  return (
    <div style={{ height, width: "100%" }} className="rounded-md border bg-slate-50/50">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background gap={16} color="#e2e8f0" />
        <Controls showInteractive={false} />
        <DagLegend />
      </ReactFlow>
    </div>
  );
}

/** Small inline legend so a presenter can explain the colors without leaving the view. */
function DagLegend() {
  const items: { color: string; label: string }[] = [
    { color: "#22c55e", label: "Completed" },
    { color: "#3b82f6", label: "Running / sleeping" },
    { color: "#ef4444", label: "Failed" },
    { color: "#f59e0b", label: "Compensating" },
  ];
  return (
    <div className="absolute bottom-3 right-3 z-10 flex flex-col gap-1 rounded-md border bg-white/90 p-2 text-xs shadow-sm">
      {items.map((i) => (
        <div key={i.label} className="flex items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: i.color }}
          />
          <span className="text-slate-600">{i.label}</span>
        </div>
      ))}
    </div>
  );
}
