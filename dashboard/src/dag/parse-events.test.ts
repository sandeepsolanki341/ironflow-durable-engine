import { describe, it, expect } from "vitest";
import { parseEventsToDag } from "./parse-events";
import type { EventView } from "@/types/execution";

// These histories mirror the backend's OrderFulfillmentSimulationTest byte-for-byte in their
// payload shapes: identity on ACTIVITY_SCHEDULED, scheduledEventSeq back-refs on outcomes,
// isCompensation+registrationSeq on compensation activities, registrationSeq on
// COMPENSATION_COMPLETED. If the parser is right against these, it is right against the
// engine.

function ev(sequenceNumber: number, eventType: string, payload: unknown): EventView {
  return { sequenceNumber, eventType, payload, createdAt: "2026-01-01T00:00:00Z" };
}

/** Happy path up to (but not including) the failure — everything green. */
const happyHistory: EventView[] = [
  ev(1, "WORKFLOW_STARTED", { input: null }),
  ev(2, "ACTIVITY_SCHEDULED", { identity: "chargeCard" }),
  ev(3, "ACTIVITY_COMPLETED", { scheduledEventSeq: 2, result: { transactionId: "txn-77" } }),
  ev(4, "COMPENSATION_REGISTERED", { identity: "refund", compensationType: "refund", input: "txn-77" }),
  ev(5, "ACTIVITY_SCHEDULED", { identity: "reserveInventory" }),
  ev(6, "ACTIVITY_COMPLETED", { scheduledEventSeq: 5, result: { reservationId: "resv-9" } }),
  ev(7, "COMPENSATION_REGISTERED", { identity: "releaseInventory", compensationType: "releaseInventory", input: "resv-9" }),
  ev(8, "TIMER_STARTED", { identity: "__timer", durationMillis: 259200000, fireAt: "2026-01-04T00:00:00Z" }),
  ev(9, "TIMER_FIRED", { scheduledEventSeq: 8 }),
  ev(10, "ACTIVITY_SCHEDULED", { identity: "sendReviewEmail" }),
  ev(11, "ACTIVITY_SCHEDULED", { identity: "sendReviewSms" }),
];

/** The rollback: SMS fails, both compensations run in reverse and complete. */
const rollbackHistory: EventView[] = [
  ...happyHistory,
  ev(12, "ACTIVITY_COMPLETED", { scheduledEventSeq: 10, result: null }),
  ev(13, "ACTIVITY_FAILED", { scheduledEventSeq: 11, failure: "SMS provider unreachable after 5 attempts", attempts: 5, maxAttempts: 5 }),
  ev(14, "COMPENSATION_TRIGGERED", { failure: "SMS provider unreachable after 5 attempts" }),
  // releaseInventory (registered at seq 7) runs first — LIFO.
  ev(15, "ACTIVITY_SCHEDULED", { identity: "releaseInventory", isCompensation: true, registrationSeq: 7 }),
  ev(16, "COMPENSATION_COMPLETED", { registrationSeq: 7, identity: "releaseInventory" }),
  // then refund (registered at seq 4).
  ev(17, "ACTIVITY_SCHEDULED", { identity: "refund", isCompensation: true, registrationSeq: 4 }),
  ev(18, "COMPENSATION_COMPLETED", { registrationSeq: 4, identity: "refund" }),
];

describe("parseEventsToDag — forward topology", () => {
  it("creates a node per forward activity plus a timer, all green on the happy path", () => {
    const dag = parseEventsToDag(happyHistory, "RUNNING");
    const labels = dag.nodes.map((n) => n.label);
    expect(labels).toContain("chargeCard");
    expect(labels).toContain("reserveInventory");
    expect(labels).toContain("sendReviewEmail");
    expect(labels).toContain("sendReviewSms");
    expect(labels.some((l) => l.startsWith("Sleep"))).toBe(true);

    // chargeCard and reserveInventory completed -> green.
    expect(dag.nodes.find((n) => n.label === "chargeCard")?.state).toBe("COMPLETED");
    expect(dag.nodes.find((n) => n.label === "reserveInventory")?.state).toBe("COMPLETED");
  });

  it("marks the fired timer COMPLETED and formats a 3-day sleep", () => {
    const dag = parseEventsToDag(happyHistory, "RUNNING");
    const timer = dag.nodes.find((n) => n.kind === "timer");
    expect(timer?.label).toBe("Sleep 3d");
    expect(timer?.state).toBe("COMPLETED");
  });

  it("leaves not-yet-completed scheduled activities ACTIVE (blue/pulsing)", () => {
    const dag = parseEventsToDag(happyHistory, "RUNNING");
    // sendReviewSms was scheduled (seq 11) but has no outcome in the happy history.
    expect(dag.nodes.find((n) => n.label === "sendReviewSms")?.state).toBe("ACTIVE");
  });
});

describe("parseEventsToDag — failure", () => {
  it("marks the failed activity red and captures the failure text + attempts", () => {
    const dag = parseEventsToDag(rollbackHistory, "FAILED_COMPENSATED");
    const sms = dag.nodes.find((n) => n.label === "sendReviewSms");
    expect(sms?.state).toBe("FAILED");
    expect(sms?.failure).toContain("SMS provider unreachable");
    expect(sms?.attempts).toBe(5);
    expect(sms?.maxAttempts).toBe(5);
  });
});

describe("parseEventsToDag — compensation (reverse flow)", () => {
  it("creates a compensation node per rollback step, linked to the node it undoes", () => {
    const dag = parseEventsToDag(rollbackHistory, "FAILED_COMPENSATED");
    const comps = dag.nodes.filter((n) => n.kind === "compensation");
    expect(comps).toHaveLength(2);

    const release = comps.find((n) => n.activityType === "releaseInventory");
    const refund = comps.find((n) => n.activityType === "refund");
    // Each compensation points back at its forward node.
    const reserveNode = dag.nodes.find((n) => n.label === "reserveInventory");
    const chargeNode = dag.nodes.find((n) => n.label === "chargeCard");
    expect(release?.compensatesNodeId).toBe(reserveNode?.id);
    expect(refund?.compensatesNodeId).toBe(chargeNode?.id);
  });

  it("draws an amber compensation edge from each forward node to its undo", () => {
    const dag = parseEventsToDag(rollbackHistory, "FAILED_COMPENSATED");
    const compEdges = dag.edges.filter((e) => e.kind === "compensation");
    expect(compEdges).toHaveLength(2);
    // Edge source is the forward node, target is the compensation node — the reverse link.
    const reserveNode = dag.nodes.find((n) => n.label === "reserveInventory")!;
    expect(compEdges.some((e) => e.source === reserveNode.id)).toBe(true);
  });

  it("settles completed compensations to COMPLETED and flips forwards to COMPENSATED", () => {
    const dag = parseEventsToDag(rollbackHistory, "FAILED_COMPENSATED");
    const comps = dag.nodes.filter((n) => n.kind === "compensation");
    // Both compensations completed in this history.
    expect(comps.every((n) => n.state === "COMPLETED")).toBe(true);
    // The forward nodes they undid are now COMPENSATED.
    expect(dag.nodes.find((n) => n.label === "reserveInventory")?.state).toBe("COMPENSATED");
    expect(dag.nodes.find((n) => n.label === "chargeCard")?.state).toBe("COMPENSATED");
    // Completed compensation edges stop animating.
    expect(dag.edges.filter((e) => e.kind === "compensation").every((e) => !e.animated)).toBe(
      true,
    );
  });

  it("keeps a still-running compensation amber and animated", () => {
    // Truncate after the first compensation is scheduled but before it completes.
    const midRollback = rollbackHistory.slice(0, 15); // up to seq 15 (releaseInventory scheduled), before its completion at seq 16
    const dag = parseEventsToDag(midRollback, "COMPENSATING");
    const release = dag.nodes.find(
      (n) => n.kind === "compensation" && n.activityType === "releaseInventory",
    );
    expect(release?.state).toBe("COMPENSATING");
    const edge = dag.edges.find((e) => e.target === release?.id && e.kind === "compensation");
    expect(edge?.animated).toBe(true);
  });
});

describe("parseEventsToDag — robustness", () => {
  it("is order-insensitive: shuffled input yields the same node states", () => {
    const shuffled = [...rollbackHistory].reverse();
    const a = parseEventsToDag(rollbackHistory, "FAILED_COMPENSATED");
    const b = parseEventsToDag(shuffled, "FAILED_COMPENSATED");
    const norm = (dag: ReturnType<typeof parseEventsToDag>) =>
      dag.nodes
        .map((n) => `${n.label}:${n.state}`)
        .sort()
        .join("|");
    expect(norm(b)).toBe(norm(a));
  });

  it("handles an empty history without throwing", () => {
    const dag = parseEventsToDag([], null);
    // Just the synthetic start node, no crash.
    expect(dag.nodes.some((n) => n.kind === "start")).toBe(true);
  });
});
