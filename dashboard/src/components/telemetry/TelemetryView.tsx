import { useMemo } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useTelemetry } from "@/api/useTelemetry";
import type { TelemetrySample } from "@/types/execution";

// The "System Telemetry" view: three live line charts fed by a rolling window of samples.
// Queue depth and dispatch latency and active threads are the three the prompt calls out; each
// is its own chart so their very different y-scales (a backlog of thousands vs. a latency of
// milliseconds) never fight for one axis.

interface ChartPoint {
  t: string;
  queueDepth: number;
  leasedTasks: number;
  dispatchLatencyMs: number | null;
  activeVirtualThreads: number | null;
}

function toPoints(samples: TelemetrySample[]): ChartPoint[] {
  return samples.map((s) => ({
    t: new Date(s.timestamp).toLocaleTimeString([], {
      minute: "2-digit",
      second: "2-digit",
    }),
    queueDepth: s.queueDepth,
    leasedTasks: s.leasedTasks,
    dispatchLatencyMs: s.dispatchLatencyMs,
    activeVirtualThreads: s.activeVirtualThreads,
  }));
}

export function TelemetryView() {
  const { samples, isLoading, isError } = useTelemetry();
  const data = useMemo(() => toPoints(samples), [samples]);

  if (isError) {
    return (
      <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
        Telemetry endpoint unreachable. Check that the engine is running and Actuator metrics
        are exposed.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <span className="relative flex h-2.5 w-2.5">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
          <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-green-500" />
        </span>
        <h2 className="text-sm font-semibold">System Telemetry</h2>
        <span className="text-xs text-muted-foreground">
          {isLoading ? "connecting…" : `${samples.length} samples · live`}
        </span>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <TelemetryChart
          title="Queue depth"
          subtitle="PENDING tasks awaiting dispatch"
          data={data}
          series={[
            { key: "queueDepth", label: "Queue depth", color: "#3b82f6" },
            { key: "leasedTasks", label: "In-flight", color: "#8b5cf6" },
          ]}
        />
        <TelemetryChart
          title="Dispatch latency"
          subtitle="mean ms, recent window"
          data={data}
          unit="ms"
          series={[
            { key: "dispatchLatencyMs", label: "Latency (ms)", color: "#f59e0b" },
          ]}
        />
        <TelemetryChart
          title="Active threads"
          subtitle="live virtual/platform threads"
          data={data}
          series={[
            {
              key: "activeVirtualThreads",
              label: "Threads",
              color: "#10b981",
            },
          ]}
        />
      </div>
    </div>
  );
}

interface SeriesDef {
  key: keyof ChartPoint;
  label: string;
  color: string;
}

function TelemetryChart({
  title,
  subtitle,
  data,
  series,
  unit,
}: {
  title: string;
  subtitle: string;
  data: ChartPoint[];
  series: SeriesDef[];
  unit?: string;
}) {
  return (
    <div className="rounded-md border p-3">
      <div className="mb-2">
        <div className="text-sm font-medium">{title}</div>
        <div className="text-xs text-muted-foreground">{subtitle}</div>
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="t" tick={{ fontSize: 10 }} minTickGap={24} />
          <YAxis
            tick={{ fontSize: 10 }}
            width={40}
            unit={unit}
            allowDecimals={false}
          />
          <Tooltip
            contentStyle={{ fontSize: 12, borderRadius: 6 }}
            labelStyle={{ fontSize: 11 }}
          />
          {series.map((s) => (
            <Line
              key={String(s.key)}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
