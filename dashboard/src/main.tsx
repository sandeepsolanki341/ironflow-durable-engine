import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ExecutionsPage } from "@/pages/ExecutionsPage";
import "./index.css";

// One QueryClient for the app. Defaults tuned for an operator dashboard: keep data a little
// stale rather than refetching on every window focus, and don't retry forever on a hard 4xx.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2000,
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ExecutionsPage />
    </QueryClientProvider>
  </React.StrictMode>,
);
