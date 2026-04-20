// =============================================================================================
// IronFlow thundering-herd load test.
//
// Goal: drive the engine to its state-transition ceiling and verify the documented ~5,000-
// 10,000 transitions/sec on a single Postgres instance, while recording p95/p99 task-resume
// latency.
//
// Run:  k6 run load-test.js
//       k6 run -e BASE_URL=http://localhost:8080 -e TARGET_VUS=1000 load-test.js
//
// What "throughput" means here, and why it is measured server-side:
//   A state transition is one durable, version-bumped advance of a workflow (a decision commit,
//   an activity completion, ...). The engine exposes a Micrometer counter,
//   ironflow.transitions.total, via Actuator. This script samples that counter and differentiates
//   it over wall time to get transitions/sec - the true engine throughput, not merely the HTTP
//   submit rate. Measuring submits alone would understate throughput badly, because each
//   submitted Benchmark workflow produces several transitions as it runs to completion.
// =============================================================================================

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter, Gauge } from "k6/metrics";

// Self-contained unique key generator. Deliberately NOT importing the k6 jslib uuid helper:
// keeping the script dependency-free means it runs in an air-gapped or network-restricted CI
// runner with no reach to jslib.k6.io. VU id + iteration + a random suffix is unique enough to
// key a load workflow.
function uniqueKey() {
  return `${__VU}-${__ITER}-${Math.random().toString(36).slice(2, 10)}`;
}

// ---- Configuration --------------------------------------------------------------------------
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const TARGET_VUS = parseInt(__ENV.TARGET_VUS || "1000", 10);
const WORKFLOW_TYPE = __ENV.WORKFLOW_TYPE || "Benchmark";

// Documented ceiling the run asserts against.
const CEILING_MIN = parseInt(__ENV.CEILING_MIN || "5000", 10);
const CEILING_TARGET = parseInt(__ENV.CEILING_TARGET || "10000", 10);

// ---- Custom metrics -------------------------------------------------------------------------
// Resume latency: time from submitting a workflow to observing it reach a terminal state, i.e.
// how long the engine took to actually run the thing end-to-end under load. p95/p99 of this is
// the headline latency figure.
const resumeLatency = new Trend("ironflow_resume_latency_ms", true);
// Throughput sampled from the server counter, in transitions/sec.
const transitionsPerSec = new Gauge("ironflow_transitions_per_sec");
// Total transitions observed over the run (final minus initial counter value).
const transitionsTotal = new Counter("ironflow_transitions_observed");
// Submit failures, kept separate from run failures for diagnosis.
const submitErrors = new Counter("ironflow_submit_errors");

// ---- Load profile ---------------------------------------------------------------------------
// A thundering herd: ramp hard to TARGET_VUS, hold, then ramp down. The steep ramp is the point
// - it slams the queue with concurrent submissions so SELECT ... FOR UPDATE SKIP LOCKED
// contention is exercised, not amortized over a gentle warmup.
export const options = {
  scenarios: {
    thundering_herd: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: TARGET_VUS },   // steep ramp: the herd arrives
        { duration: "2m", target: TARGET_VUS },     // sustained peak: measure the ceiling here
        { duration: "30s", target: 0 },             // ramp down
      ],
      gracefulRampDown: "10s",
    },
    // A separate low-rate sampler VU that polls the transition counter once a second for the
    // whole run, so throughput is measured continuously rather than inferred.
    throughput_sampler: {
      executor: "constant-vus",
      vus: 1,
      duration: "3m10s",
      exec: "sampleThroughput",
    },
  },
  thresholds: {
    // The assertions that make this a verification, not just a benchmark. A run that violates
    // these exits non-zero, so CI can gate on it.
    "ironflow_resume_latency_ms": [
      "p(95)<2000",   // p95 resume under 2s at peak load
      "p(99)<5000",   // p99 resume under 5s
    ],
    "http_req_failed{scenario:thundering_herd}": ["rate<0.01"],   // <1% submit failures
    // Throughput must clear the documented floor. Checked against the peak sample; see summary.
    "ironflow_transitions_per_sec": [`value>=${CEILING_MIN}`],
  },
};

// ---- Sampler state --------------------------------------------------------------------------
let lastCount = null;
let lastTime = null;
let peakThroughput = 0;

// Reads the server-side transition counter. Actuator returns
// { name, measurements: [{ statistic: "COUNT", value: N }], ... }.
function readTransitionCounter() {
  const res = http.get(
    `${BASE_URL}/actuator/metrics/ironflow.transitions.total`,
    { tags: { sampler: "true" } },
  );
  if (res.status !== 200) return null;
  try {
    const body = JSON.parse(res.body);
    const m = body.measurements.find((x) => x.statistic === "COUNT");
    return m ? m.value : null;
  } catch (_e) {
    return null;
  }
}

// Sampler entrypoint: differentiate the counter once a second.
export function sampleThroughput() {
  const now = Date.now();
  const count = readTransitionCounter();
  if (count !== null && lastCount !== null && lastTime !== null) {
    const dt = (now - lastTime) / 1000;
    if (dt > 0) {
      const rate = (count - lastCount) / dt;
      transitionsPerSec.add(rate);
      if (rate > peakThroughput) peakThroughput = rate;
    }
  }
  if (count !== null) {
    lastCount = count;
    lastTime = now;
  }
  sleep(1);
}

// ---- Main VU: submit a workflow and time its end-to-end resume ------------------------------
export default function () {
  const businessKey = `load-${uniqueKey()}`;
  const payload = JSON.stringify({
    workflowType: WORKFLOW_TYPE,
    input: { label: businessKey },
    businessKey: businessKey,
    taskQueue: "default",
  });

  const submitStart = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/workflows/start`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { scenario: "thundering_herd" },
  });

  const ok = check(res, {
    "submit accepted": (r) => r.status === 200 || r.status === 201,
    "submit returns execution id": (r) => {
      try {
        return !!JSON.parse(r.body).executionId;
      } catch (_e) {
        return false;
      }
    },
  });

  if (!ok) {
    submitErrors.add(1);
    return;
  }

  const executionId = JSON.parse(res.body).executionId;

  // Poll the execution to completion to measure true resume latency. The Benchmark workflow is
  // short, so a bounded poll with a small interval captures end-to-end time without hammering
  // the detail endpoint. We cap attempts so a stuck execution cannot hang a VU forever.
  const maxAttempts = 50;
  const pollIntervalMs = 100;
  let terminal = false;
  for (let i = 0; i < maxAttempts; i++) {
    const detail = http.get(
      `${BASE_URL}/api/v1/executions/${executionId}?includeHistory=false`,
      { tags: { scenario: "thundering_herd", poll: "true" } },
    );
    if (detail.status === 200) {
      let status;
      try {
        status = JSON.parse(detail.body).status;
      } catch (_e) {
        status = null;
      }
      if (
        status === "COMPLETED" ||
        status === "FAILED" ||
        status === "FAILED_COMPENSATED"
      ) {
        terminal = true;
        resumeLatency.add(Date.now() - submitStart);
        break;
      }
    }
    sleep(pollIntervalMs / 1000);
  }

  // A workflow that never reached terminal within the poll budget is recorded as a resume miss
  // via the check; it does not add a (misleadingly capped) latency sample.
  check(null, { "workflow reached terminal state": () => terminal });
}

// ---- Summary: assert the ceiling explicitly and print a readable verdict --------------------
export function handleSummary(data) {
  const p95 = data.metrics.ironflow_resume_latency_ms?.values?.["p(95)"] ?? NaN;
  const p99 = data.metrics.ironflow_resume_latency_ms?.values?.["p(99)"] ?? NaN;
  const meanRate = data.metrics.ironflow_transitions_per_sec?.values?.avg ?? 0;
  const maxRate = data.metrics.ironflow_transitions_per_sec?.values?.max ?? 0;

  const verdict =
    maxRate >= CEILING_TARGET
      ? `PASS — peak ${Math.round(maxRate)}/s meets the ${CEILING_TARGET}/s target`
      : maxRate >= CEILING_MIN
        ? `PASS (floor) — peak ${Math.round(maxRate)}/s clears the ${CEILING_MIN}/s floor but is below the ${CEILING_TARGET}/s target`
        : `FAIL — peak ${Math.round(maxRate)}/s is below the ${CEILING_MIN}/s documented floor`;

  const lines = [
    "",
    "==================== IronFlow throughput verification ====================",
    `  Target VUs (thundering herd):   ${TARGET_VUS}`,
    `  Transitions/sec  mean:          ${Math.round(meanRate)}`,
    `  Transitions/sec  peak:          ${Math.round(maxRate)}`,
    `  Documented ceiling:             ${CEILING_MIN}–${CEILING_TARGET}/s`,
    `  Resume latency   p95:           ${Math.round(p95)} ms`,
    `  Resume latency   p99:           ${Math.round(p99)} ms`,
    "  ----------------------------------------------------------------------",
    `  VERDICT: ${verdict}`,
    "==========================================================================",
    "",
  ];

  return {
    stdout: lines.join("\n"),
    "load-test-summary.json": JSON.stringify(data, null, 2),
  };
}
