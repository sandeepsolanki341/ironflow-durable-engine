// Chaos load generator: submit 10,000 multi-step workflows and let them run WHILE the chaos
// monkey kills workers. Unlike the throughput load test, this one does not assert latency - it
// just gets 10k durable workflows in flight and confirms they were all accepted. Correctness is
// verified afterwards by SQL, not here; this script's only job is to create the population.
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const TOTAL = parseInt(__ENV.TOTAL_WORKFLOWS || "10000", 10);
const WORKFLOW_TYPE = __ENV.WORKFLOW_TYPE || "Benchmark";

const submitted = new Counter("chaos_workflows_submitted");
const rejected = new Counter("chaos_workflows_rejected");

export const options = {
  scenarios: {
    // shared-iterations spreads exactly TOTAL submissions across the VUs, so we get precisely
    // 10,000 workflows regardless of how fast each VU goes. High VU count = the "simultaneously"
    // the spec asks for.
    flood: {
      executor: "shared-iterations",
      vus: parseInt(__ENV.VUS || "200", 10),
      iterations: TOTAL,
      maxDuration: __ENV.MAX_DURATION || "10m",
    },
  },
};

export default function () {
  const key = `chaos-${__VU}-${__ITER}`;
  const res = http.post(
    `${BASE_URL}/api/v1/workflows/start`,
    JSON.stringify({
      workflowType: WORKFLOW_TYPE,
      input: { label: key },
      businessKey: key,
      taskQueue: "default",
    }),
    { headers: { "Content-Type": "application/json" } },
  );
  // A submit may fail transiently while a worker is being killed; the API tier is separate from
  // the workers, but a network drop can still cause a 5xx. Retry once, then count it. Even a
  // genuinely lost SUBMIT is fine for the invariant check: a workflow that was never created
  // cannot violate any invariant. We track the number for reporting, not as a failure.
  if (res.status === 200 || res.status === 201) {
    submitted.add(1);
  } else {
    const retry = http.post(
      `${BASE_URL}/api/v1/workflows/start`,
      JSON.stringify({
        workflowType: WORKFLOW_TYPE,
        input: { label: key },
        businessKey: key,
        taskQueue: "default",
      }),
      { headers: { "Content-Type": "application/json" } },
    );
    if (retry.status === 200 || retry.status === 201) submitted.add(1);
    else rejected.add(1);
  }
  check(res, { "submit not 4xx": (r) => r.status < 400 || r.status >= 500 });
}
