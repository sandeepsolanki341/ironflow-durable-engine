// Thin, typed fetch wrapper. One place that knows the base URL, error shape, and query
// serialization — every hook goes through here so the contract is enforced in exactly one
// spot rather than duplicated across call sites.

const BASE = "/api/v1";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    public readonly body?: unknown,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/** Drops null/undefined params and builds a query string; empty -> "". */
function toQuery(params: Record<string, unknown>): string {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== null && v !== undefined && v !== "") {
      usp.set(k, String(v));
    }
  }
  const s = usp.toString();
  return s ? `?${s}` : "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });

  if (!res.ok) {
    // Try to surface the backend's error body, but never throw while building the error.
    let body: unknown = undefined;
    try {
      body = await res.json();
    } catch {
      /* non-JSON error body; leave undefined */
    }
    const detail =
      body && typeof body === "object" && "message" in body
        ? String((body as { message: unknown }).message)
        : res.statusText;
    throw new ApiError(res.status, `${res.status} ${detail}`, body);
  }

  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  get: <T>(path: string, params?: Record<string, unknown>) =>
    request<T>(`${path}${params ? toQuery(params) : ""}`),
};
