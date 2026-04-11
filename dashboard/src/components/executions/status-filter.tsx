import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { ExecutionStatus } from "@/types/execution";

// The prompt calls out these four as the primary operational filters. We surface them as
// toggle badges plus an "All" reset; the full enum is still filterable via the type contract,
// but these are the ones an operator reaches for.
const PRIMARY_FILTERS: ExecutionStatus[] = [
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "COMPENSATING",
];

const FILTER_STYLES: Record<string, { on: string; off: string }> = {
  RUNNING: { on: "bg-blue-600 text-white border-blue-600", off: "text-blue-700 border-blue-200 hover:bg-blue-50" },
  COMPLETED: { on: "bg-green-600 text-white border-green-600", off: "text-green-700 border-green-200 hover:bg-green-50" },
  FAILED: { on: "bg-red-600 text-white border-red-600", off: "text-red-700 border-red-200 hover:bg-red-50" },
  COMPENSATING: { on: "bg-amber-500 text-white border-amber-500", off: "text-amber-800 border-amber-200 hover:bg-amber-50" },
};

interface StatusFilterProps {
  value: ExecutionStatus | null;
  onChange: (status: ExecutionStatus | null) => void;
}

export function StatusFilter({ value, onChange }: StatusFilterProps) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <button type="button" onClick={() => onChange(null)}>
        <Badge
          variant="outline"
          className={cn(
            "cursor-pointer",
            value === null
              ? "bg-primary text-primary-foreground border-primary"
              : "text-foreground hover:bg-accent",
          )}
        >
          All
        </Badge>
      </button>
      {PRIMARY_FILTERS.map((status) => {
        const active = value === status;
        const style = FILTER_STYLES[status];
        return (
          <button
            key={status}
            type="button"
            onClick={() => onChange(active ? null : status)}
            aria-pressed={active}
          >
            <Badge
              variant="outline"
              className={cn("cursor-pointer", active ? style.on : style.off)}
            >
              {status.charAt(0) + status.slice(1).toLowerCase()}
            </Badge>
          </button>
        );
      })}
    </div>
  );
}
