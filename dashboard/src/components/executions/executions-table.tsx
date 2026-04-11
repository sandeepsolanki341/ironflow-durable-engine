import { useEffect, useMemo, useState } from "react";
import { Search } from "lucide-react";
import { useExecutions } from "@/api/executions";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ExecutionStatus } from "@/types/execution";
import { StatusBadge } from "./status-badge";
import { StatusFilter } from "./status-filter";
import { formatDuration } from "./duration";

/**
 * The Executions Overview: the dashboard's front door.
 *
 * <p>Wires four things to a single server query: the status filter badges, a debounced
 * business-key search, offset pagination, and a live-ticking clock for in-flight durations.
 * Filter and search changes reset to page 0, because "page 3 of the old filter" is never what
 * the operator means after changing the filter.</p>
 */
export function ExecutionsTable({
  onSelect,
}: {
  onSelect?: (executionId: string) => void;
}) {
  const [status, setStatus] = useState<ExecutionStatus | null>(null);
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [page, setPage] = useState(0);
  const size = 25;

  // Debounce the search box so we don't fire a query on every keystroke.
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300);
    return () => clearTimeout(t);
  }, [search]);

  // Any filter change returns to the first page.
  useEffect(() => {
    setPage(0);
  }, [status, debouncedSearch]);

  const { data, isLoading, isError, error, isFetching } = useExecutions({
    status,
    businessKey: debouncedSearch || null,
    page,
    size,
  });

  // A ticking "now" so live (non-terminal) rows show a growing duration without a refetch.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const rows = data?.items ?? [];
  const totalPages = data?.totalPages ?? 1;
  const totalItems = data?.totalItems ?? 0;

  const rangeLabel = useMemo(() => {
    if (totalItems === 0) return "No executions";
    const first = page * size + 1;
    const last = Math.min(totalItems, (page + 1) * size);
    return `${first}–${last} of ${totalItems}`;
  }, [page, size, totalItems]);

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <StatusFilter value={status} onChange={setStatus} />
        <div className="relative w-full sm:w-72">
          <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search by business key…"
            className="pl-8"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Status</TableHead>
              <TableHead>Workflow</TableHead>
              <TableHead>Business key</TableHead>
              <TableHead>Started</TableHead>
              <TableHead className="text-right">Duration</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                  Loading executions…
                </TableCell>
              </TableRow>
            ) : isError ? (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center text-destructive">
                  Failed to load: {error instanceof Error ? error.message : "unknown error"}
                </TableCell>
              </TableRow>
            ) : rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                  No executions match these filters.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row) => (
                <TableRow
                  key={row.executionId}
                  className={onSelect ? "cursor-pointer" : undefined}
                  onClick={() => onSelect?.(row.executionId)}
                >
                  <TableCell>
                    <StatusBadge status={row.status} />
                  </TableCell>
                  <TableCell className="font-medium">{row.workflowType}</TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {row.businessKey ?? "—"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(row.startTime).toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatDuration(row.startTime, row.endTime, row.status, now)}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between text-sm text-muted-foreground">
        <span>
          {rangeLabel}
          {isFetching && !isLoading ? " · refreshing…" : ""}
        </span>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
          >
            Previous
          </Button>
          <span className="tabular-nums">
            Page {page + 1} of {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages - 1}
            onClick={() => setPage((p) => p + 1)}
          >
            Next
          </Button>
        </div>
      </div>
    </div>
  );
}
