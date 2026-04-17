import { describe, it, expect, beforeEach } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import { useExecutionStream } from "./useExecutionStream";
import { executionKeys } from "./executions";
import type { ExecutionDetail, EventView } from "@/types/execution";

// A minimal fake EventSource so the hook's SSE handling can be tested without a real server.
// Captures listeners and lets the test drive "event"/"open"/"ready"/"error" frames.
class FakeEventSource {
  static last: FakeEventSource | null = null;
  url: string;
  listeners: Record<string, ((e: MessageEvent) => void)[]> = {};
  onerror: (() => void) | null = null;
  closed = false;
  constructor(url: string) {
    this.url = url;
    FakeEventSource.last = this;
  }
  addEventListener(type: string, cb: (e: MessageEvent) => void) {
    (this.listeners[type] ??= []).push(cb);
  }
  emit(type: string, data?: unknown) {
    const evt = { data: JSON.stringify(data) } as MessageEvent;
    (this.listeners[type] ?? []).forEach((cb) => cb(evt));
  }
  emitError() {
    this.onerror?.();
  }
  close() {
    this.closed = true;
  }
}

const EXEC = "11111111-1111-1111-1111-111111111111";

function wrapper(qc: QueryClient) {
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

function seedDetail(qc: QueryClient, history: EventView[]) {
  const detail: ExecutionDetail = {
    executionId: EXEC,
    workflowType: "OrderFulfillment",
    businessKey: "ord-1",
    status: "RUNNING",
    currentVersion: 1,
    startTime: "2026-01-01T00:00:00Z",
    endTime: null,
    result: null,
    failure: null,
    divergenceDetail: null,
    history,
  };
  qc.setQueryData(executionKeys.detail(EXEC), detail);
}

function ev(seq: number, type: string): EventView {
  return { sequenceNumber: seq, eventType: type, payload: {}, createdAt: "2026-01-01T00:00:00Z" };
}

describe("useExecutionStream", () => {
  beforeEach(() => {
    // @ts-expect-error install fake
    globalThis.EventSource = FakeEventSource;
    FakeEventSource.last = null;
  });

  it("opens a stream to the right URL and reports open on ready", async () => {
    const qc = new QueryClient();
    seedDetail(qc, []);
    const { result } = renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });

    expect(FakeEventSource.last?.url).toBe(`/api/v1/executions/${EXEC}/stream`);
    act(() => FakeEventSource.last!.emit("ready", 0));
    await waitFor(() => expect(result.current.status).toBe("open"));
  });

  it("appends a pushed event to the cached detail history", async () => {
    const qc = new QueryClient();
    seedDetail(qc, [ev(1, "WORKFLOW_STARTED")]);
    renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });

    act(() => FakeEventSource.last!.emit("event", ev(2, "ACTIVITY_SCHEDULED")));

    await waitFor(() => {
      const d = qc.getQueryData<ExecutionDetail>(executionKeys.detail(EXEC));
      expect(d?.history.map((e) => e.sequenceNumber)).toEqual([1, 2]);
    });
  });

  it("deduplicates a replayed event after reconnect (idempotent)", async () => {
    const qc = new QueryClient();
    seedDetail(qc, [ev(1, "WORKFLOW_STARTED")]);
    renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });

    act(() => FakeEventSource.last!.emit("event", ev(2, "ACTIVITY_SCHEDULED")));
    act(() => FakeEventSource.last!.emit("event", ev(2, "ACTIVITY_SCHEDULED"))); // duplicate

    await waitFor(() => {
      const d = qc.getQueryData<ExecutionDetail>(executionKeys.detail(EXEC));
      expect(d?.history.filter((e) => e.sequenceNumber === 2)).toHaveLength(1);
    });
  });

  it("keeps history ordered when events arrive out of order", async () => {
    const qc = new QueryClient();
    seedDetail(qc, [ev(1, "WORKFLOW_STARTED")]);
    renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });

    act(() => FakeEventSource.last!.emit("event", ev(3, "ACTIVITY_COMPLETED")));
    act(() => FakeEventSource.last!.emit("event", ev(2, "ACTIVITY_SCHEDULED")));

    await waitFor(() => {
      const d = qc.getQueryData<ExecutionDetail>(executionKeys.detail(EXEC));
      expect(d?.history.map((e) => e.sequenceNumber)).toEqual([1, 2, 3]);
    });
  });

  it("closes the EventSource on unmount", () => {
    const qc = new QueryClient();
    seedDetail(qc, []);
    const { unmount } = renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });
    const src = FakeEventSource.last!;
    unmount();
    expect(src.closed).toBe(true);
  });

  it("reflects a transient drop as connecting", async () => {
    const qc = new QueryClient();
    seedDetail(qc, []);
    const { result } = renderHook(() => useExecutionStream(EXEC), { wrapper: wrapper(qc) });
    act(() => FakeEventSource.last!.emit("ready", 0));
    await waitFor(() => expect(result.current.status).toBe("open"));
    act(() => FakeEventSource.last!.emitError());
    await waitFor(() => expect(result.current.status).toBe("connecting"));
  });
});
