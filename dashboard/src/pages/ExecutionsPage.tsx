import { useState } from "react";
import { ExecutionsTable } from "@/components/executions/executions-table";
import { ExecutionDetailPanel } from "./ExecutionDetailPanel";
import { TelemetryView } from "@/components/telemetry/TelemetryView";
import { cn } from "@/lib/utils";

type Tab = "executions" | "telemetry";

export function ExecutionsPage() {
  const [tab, setTab] = useState<Tab>("executions");
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b">
        <div className="mx-auto max-w-6xl px-6 py-4">
          <h1 className="text-lg font-semibold tracking-tight">IronFlow</h1>
          <nav className="mt-2 flex gap-1">
            <TabButton active={tab === "executions"} onClick={() => setTab("executions")}>
              Executions
            </TabButton>
            <TabButton active={tab === "telemetry"} onClick={() => setTab("telemetry")}>
              Telemetry
            </TabButton>
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-6 py-6">
        {tab === "telemetry" ? (
          <TelemetryView />
        ) : selected ? (
          <ExecutionDetailPanel executionId={selected} onBack={() => setSelected(null)} />
        ) : (
          <ExecutionsTable onSelect={setSelected} />
        )}
      </main>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
        active
          ? "bg-primary text-primary-foreground"
          : "text-muted-foreground hover:bg-accent hover:text-foreground",
      )}
    >
      {children}
    </button>
  );
}
