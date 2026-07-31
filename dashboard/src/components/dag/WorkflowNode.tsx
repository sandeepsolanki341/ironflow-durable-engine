import { memo } from "react";
import { Handle, Position } from "reactflow";
import {
  Check,
  X,
  Clock,
  Undo2,
  Play,
  Flag,
  CircleDot,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { NodeKind, NodeState } from "@/dag/types";

// The visual heart of the feature. Each state maps to a fixed treatment matching the prompt:
//   COMPLETED    -> green fill + check icon
//   ACTIVE       -> blue border that PULSES (running activity or a live sleep)
//   FAILED       -> red fill + X
//   COMPENSATING -> amber border that pulses (undo in flight)
//   COMPENSATED  -> settled amber (undo done)
// The pulse is a Tailwind animate-pulse on the border via a ring utility, so no custom CSS.

interface NodeData {
  label: string;
  state: NodeState;
  kind: NodeKind;
  failure?: string;
  attempts?: number;
  maxAttempts?: number;
}

const STATE_CLASSES: Record<NodeState, string> = {
  PENDING: "bg-white border-slate-300 text-slate-600",
  ACTIVE: "bg-blue-50 border-blue-500 text-blue-900 ring-4 ring-blue-200 animate-pulse",
  COMPLETED: "bg-green-50 border-green-500 text-green-900",
  FAILED: "bg-red-50 border-red-500 text-red-900",
  COMPENSATING:
    "bg-amber-50 border-amber-500 text-amber-900 ring-4 ring-amber-200 animate-pulse",
  COMPENSATED: "bg-orange-50 border-orange-400 text-orange-900",
};

function StateIcon({ state, kind }: { state: NodeState; kind: NodeKind }) {
  if (kind === "start") return <Play className="h-3.5 w-3.5" />;
  if (kind === "end") return <Flag className="h-3.5 w-3.5" />;
  if (kind === "compensation") return <Undo2 className="h-3.5 w-3.5" />;
  if (kind === "timer" && state !== "COMPLETED") return <Clock className="h-3.5 w-3.5" />;
  switch (state) {
    case "COMPLETED":
      return <Check className="h-3.5 w-3.5" />;
    case "FAILED":
      return <X className="h-3.5 w-3.5" />;
    case "ACTIVE":
      return <CircleDot className="h-3.5 w-3.5" />;
    default:
      return <CircleDot className="h-3.5 w-3.5" />;
  }
}

function WorkflowNodeImpl({ data }: { data: NodeData }) {
  const { label, state, kind, failure, attempts, maxAttempts } = data;
  return (
    <div
      className={cn(
        "min-w-[150px] rounded-lg border-2 px-3 py-2 shadow-sm transition-colors",
        STATE_CLASSES[state],
      )}
      title={failure ?? undefined}
    >
      {/* Handles: forward nodes connect left->right; compensation nodes also accept a top
          handle so the amber reverse edge can drop into them from the forward node above. */}
      <Handle type="target" position={Position.Left} className="!bg-slate-400" />
      {kind === "compensation" && (
        <Handle
          type="target"
          position={Position.Top}
          id="undo"
          className="!bg-amber-500"
        />
      )}
      <div className="flex items-center gap-1.5">
        <StateIcon state={state} kind={kind} />
        <span className="truncate text-sm font-medium">{label}</span>
      </div>
      {attempts !== undefined && maxAttempts !== undefined && state === "FAILED" && (
        <div className="mt-0.5 text-[10px] opacity-70">
          {attempts}/{maxAttempts} attempts
        </div>
      )}
      {failure && (
        <div className="mt-0.5 max-w-[160px] truncate text-[10px] opacity-70">{failure}</div>
      )}
      <Handle type="source" position={Position.Right} className="!bg-slate-400" />
    </div>
  );
}

export const WorkflowNode = memo(WorkflowNodeImpl);
