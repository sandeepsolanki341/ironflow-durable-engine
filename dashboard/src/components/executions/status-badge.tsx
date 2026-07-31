import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { ExecutionStatus } from "@/types/execution";

// Each status gets a fixed, meaningful color. The mapping is exhaustive over ExecutionStatus,
// so adding a status to the enum without giving it a color is a compile error, not a silent
// grey badge in production.
const STATUS_STYLES: Record<ExecutionStatus, string> = {
  RUNNING: "bg-blue-100 text-blue-800 border-blue-200",
  COMPLETED: "bg-green-100 text-green-800 border-green-200",
  FAILED: "bg-red-100 text-red-800 border-red-200",
  COMPENSATING: "bg-amber-100 text-amber-900 border-amber-200",
  FAILED_COMPENSATED: "bg-orange-100 text-orange-800 border-orange-200",
  DIVERGENT: "bg-purple-100 text-purple-800 border-purple-200",
  CANCELLED: "bg-gray-100 text-gray-700 border-gray-200",
  TIMED_OUT: "bg-rose-100 text-rose-800 border-rose-200",
};

const STATUS_LABELS: Record<ExecutionStatus, string> = {
  RUNNING: "Running",
  COMPLETED: "Completed",
  FAILED: "Failed",
  COMPENSATING: "Compensating",
  FAILED_COMPENSATED: "Rolled back",
  DIVERGENT: "Divergent",
  CANCELLED: "Cancelled",
  TIMED_OUT: "Timed out",
};

export function StatusBadge({ status }: { status: ExecutionStatus }) {
  return (
    <Badge variant="outline" className={cn("font-medium", STATUS_STYLES[status])}>
      {STATUS_LABELS[status]}
    </Badge>
  );
}
