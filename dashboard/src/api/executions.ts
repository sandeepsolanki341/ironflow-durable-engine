import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { api } from "./client";
import type {
  ExecutionDetail,
  ExecutionSummary,
  ListExecutionsParams,
  PageResponse,
} from "@/types/execution";

// Centralized, typed query keys. Everything that reads executions derives its cache key from
// here, so an invalidation (e.g. after a signal) can target exactly the right slice of cache.
export const executionKeys = {
  all: ["executions"] as const,
  lists: () => [...executionKeys.all, "list"] as const,
  list: (params: ListExecutionsParams) =>
    [...executionKeys.lists(), params] as const,
  details: () => [...executionKeys.all, "detail"] as const,
  detail: (id: string) => [...executionKeys.details(), id] as const,
};

/**
 * Paginated, filtered executions list.
 *
 * <p>keepPreviousData holds the current page visible while the next filter/page loads, so the
 * table does not flash empty on every keystroke or page change. placeholderData is the v5 way
 * to express that.</p>
 *
 * <p>Live executions change server-side without any client action, so the list refetches on a
 * modest interval. The interval is deliberately coarse (5s): this is an operator dashboard,
 * not a trading screen, and a tighter loop would hammer the count query for no real benefit.</p>
 */
export function useExecutions(params: ListExecutionsParams) {
  return useQuery({
    queryKey: executionKeys.list(params),
    queryFn: () =>
      api.get<PageResponse<ExecutionSummary>>("/executions", {
        status: params.status,
        businessKey: params.businessKey,
        page: params.page ?? 0,
        size: params.size ?? 25,
      }),
    placeholderData: keepPreviousData,
    refetchInterval: 5000,
  });
}

/**
 * Full detail for one execution, including replay history.
 *
 * <p>Refetches while the execution is non-terminal (history is still growing) and stops once
 * it settles, via a function-form refetchInterval that returns false on a terminal status.
 * A completed execution's history is immutable, so polling it forever would be pure waste.</p>
 */
export function useExecutionDetail(id: string | null, includeHistory = true) {
  return useQuery({
    queryKey: executionKeys.detail(id ?? "none"),
    queryFn: () =>
      api.get<ExecutionDetail>(`/executions/${id}`, {
        includeHistory,
      }),
    enabled: !!id,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (!status) return 3000;
      const live =
        status === "RUNNING" ||
        status === "COMPENSATING" ||
        status === "DIVERGENT";
      return live ? 3000 : false;
    },
  });
}
