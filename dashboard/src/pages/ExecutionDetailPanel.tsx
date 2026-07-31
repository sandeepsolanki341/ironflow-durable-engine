import { useExecutionDetail } from "@/api/executions";
import { useExecutionStream } from "@/api/useExecutionStream";
import { WorkflowDagViewer } from "@/components/dag/WorkflowDagViewer";
import { StatusBadge } from "@/components/executions/status-badge";
import { Button } from "@/components/ui/button";

/**
 * Detail view for one execution: header metadata + the live DAG. The detail hook polls while
 * the execution is non-terminal, so the graph updates itself as new events land — no manual
 * refresh, no websocket needed for the demo.
 */
export function ExecutionDetailPanel({
  executionId,
  onBack,
}: {
  executionId: string;
  onBack: () => void;
}) {
  const { data, isLoading, isError, error } = useExecutionDetail(executionId);
  // Open the live SSE stream; it patches the detail cache in place, so the DAG below recolors
  // itself as events arrive. The returned status drives the live/offline indicator.
  const { status: streamStatus } = useExecutionStream(executionId);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Button variant="outline" size="sm" onClick={onBack}>
          ← Back
        </Button>
        {data && <StatusBadge status={data.status} />}
        <span className="font-mono text-xs text-muted-foreground">{executionId}</span>
        <LiveDot status={streamStatus} />
      </div>

      {isLoading ? (
        <div className="flex h-96 items-center justify-center text-muted-foreground">
          Loading execution…
        </div>
      ) : isError ? (
        <div className="flex h-96 items-center justify-center text-destructive">
          {error instanceof Error ? error.message : "Failed to load execution"}
        </div>
      ) : data ? (
        <>
          <div className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
            <Meta label="Workflow" value={data.workflowType} />
            <Meta label="Business key" value={data.businessKey ?? "—"} mono />
            <Meta label="Version" value={String(data.currentVersion)} />
            <Meta label="Events" value={String(data.history.length)} />
          </div>
          {data.failure && (
            <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
              {data.failure}
            </div>
          )}
          <WorkflowDagViewer events={data.history} status={data.status} height={520} />
        </>
      ) : null}
    </div>
  );
}

function LiveDot({ status }: { status: "connecting" | "open" | "closed" }) {
  const color =
    status === "open" ? "bg-green-500" : status === "connecting" ? "bg-amber-500" : "bg-slate-300";
  const label =
    status === "open" ? "Live" : status === "connecting" ? "Connecting" : "Offline";
  return (
    <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
      <span className={`inline-block h-2 w-2 rounded-full ${color}`} />
      {label}
    </span>
  );
}

function Meta({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className={mono ? "font-mono text-xs" : "font-medium"}>{value}</div>
    </div>
  );
}
