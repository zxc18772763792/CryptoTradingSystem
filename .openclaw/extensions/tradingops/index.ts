import { Type } from "@sinclair/typebox";

function normalizeBaseUrl(raw: string): string {
  const base = String(raw || "http://127.0.0.1:8711").replace(/\/+$/, "");
  return /\/ops$/i.test(base) ? base : `${base}/ops`;
}

function getPluginConfig(api: any) {
  const cfg = api?.config?.plugins?.entries?.tradingops?.config || {};
  return {
    baseUrl: normalizeBaseUrl(cfg.baseUrl || "http://127.0.0.1:8711"),
    token: String(cfg.token || "").trim(),
    timeoutMs: Math.max(1000, Number(cfg.timeoutMs || 15000)),
  };
}

async function opsFetch(api: any, path: string, init: any = {}, extraHeaders: Record<string, string> = {}) {
  const cfg = getPluginConfig(api);
  if (!cfg.token) {
    return { ok: false, status: 0, body: { error: "tradingops token is not configured" } };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), cfg.timeoutMs);
  try {
    const resp = await fetch(`${cfg.baseUrl}${path}`, {
      method: init.method || "GET",
      body: init.body,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        "X-OPS-TOKEN": cfg.token,
        "X-OPS-CALLER": "openclaw",
        ...extraHeaders,
      },
    });
    const body = await resp.json().catch(() => ({ error: `non-json response (${resp.status})` }));
    return { ok: resp.ok, status: resp.status, body };
  } catch (err: any) {
    if (err?.name === "AbortError") {
      return { ok: false, status: 0, body: { error: `Ops API timeout after ${cfg.timeoutMs}ms` } };
    }
    return { ok: false, status: 0, body: { error: String(err?.message || err || "unknown error") } };
  } finally {
    clearTimeout(timer);
  }
}

function textResult(title: string, payload: any) {
  const text = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  return { content: [{ type: "text", text: `${title}\n${text}` }] };
}

async function callTool(api: any, title: string, path: string, method = "GET", payload: any = undefined, extraHeaders: Record<string, string> = {}) {
  const res = await opsFetch(api, path, {
    method,
    body: payload === undefined ? undefined : JSON.stringify(payload),
  }, extraHeaders);
  if (!res.ok) {
    const msg = res.body?.error || res.body?.detail || `Ops API request failed: ${res.status}`;
    return textResult(title, `Ops API request failed: ${msg}`);
  }
  return textResult(title, res.body);
}

export default async function register(api: any) {
  api.registerTool({
    name: "tradingops_status",
    description: "Read current trading/news/risk/exchange status from the Ops API.",
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_status", "/status", "GET"),
  });

  api.registerTool({
    name: "tradingops_news_pull",
    description: "Run one immediate news pull through the Ops API.",
    optional: true,
    inputSchema: Type.Object({
      since_minutes: Type.Optional(Type.Number({ minimum: 15, maximum: 1440 })),
      max_records: Type.Optional(Type.Number({ minimum: 10, maximum: 500 })),
      query: Type.Optional(Type.String()),
    }),
    execute: async (input: any) => callTool(api, "tradingops_news_pull", "/news/pull_now", "POST", input || {}),
  });

  api.registerTool({
    name: "tradingops_worker_run_once",
    description: "Run one pull/LLM worker cycle for selected news sources.",
    optional: true,
    inputSchema: Type.Object({
      sources: Type.Optional(Type.Array(Type.String())),
      llm_limit: Type.Optional(Type.Number({ minimum: 1, maximum: 50 })),
      pull_only: Type.Optional(Type.Boolean()),
      llm_only: Type.Optional(Type.Boolean()),
    }),
    execute: async (input: any) => callTool(api, "tradingops_worker_run_once", "/news/worker_run_once", "POST", input || {}),
  });

  api.registerTool({
    name: "tradingops_research_run",
    description: "Run or queue strategy research through the Ops API.",
    optional: true,
    inputSchema: Type.Object({
      exchange: Type.Optional(Type.String()),
      symbol: Type.Optional(Type.String()),
      days: Type.Optional(Type.Number({ minimum: 1 })),
      timeframes: Type.Optional(Type.Array(Type.String())),
      strategies: Type.Optional(Type.Array(Type.String())),
      commission_rate: Type.Optional(Type.Number({ minimum: 0 })),
      slippage_bps: Type.Optional(Type.Number({ minimum: 0 })),
      initial_capital: Type.Optional(Type.Number({ minimum: 1 })),
      background: Type.Optional(Type.Boolean()),
    }),
    execute: async (input: any) => callTool(api, "tradingops_research_run", "/research/run", "POST", input || {}),
  });

  api.registerTool({
    name: "tradingops_research_job_status",
    description: "Get background research job status.",
    inputSchema: Type.Object({ job_id: Type.String() }),
    execute: async (input: any) => callTool(api, "tradingops_research_job_status", `/research/job/${encodeURIComponent(String(input?.job_id || ""))}`, "GET"),
  });

  api.registerTool({
    name: "tradingops_research_latest",
    description: "Read the latest research summary cached by the Ops API.",
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_research_latest", "/research/latest", "GET"),
  });

  api.registerTool({
    name: "tradingops_trading_start_paper",
    description: "Switch the execution engine into paper mode and start it.",
    optional: true,
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_trading_start_paper", "/trading/start_paper", "POST", {}),
  });

  api.registerTool({
    name: "tradingops_trading_arm_live",
    description: "Request a one-time live approval code.",
    optional: true,
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_trading_arm_live", "/trading/arm_live", "POST", {}),
  });

  api.registerTool({
    name: "tradingops_trading_start_live",
    description: "Start live trading using a valid approval code.",
    optional: true,
    inputSchema: Type.Object({ approval_code: Type.String() }),
    execute: async (input: any) => callTool(api, "tradingops_trading_start_live", "/trading/start_live", "POST", {}, {
      "X-OPS-APPROVAL": String(input?.approval_code || ""),
    }),
  });

  api.registerTool({
    name: "tradingops_trading_stop",
    description: "Stop the execution engine.",
    optional: true,
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_trading_stop", "/trading/stop", "POST", {}),
  });

  api.registerTool({
    name: "tradingops_kill_switch",
    description: "Trigger kill switch: stop engine, cancel orders, and close positions.",
    optional: true,
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_kill_switch", "/trading/kill_switch", "POST", {}),
  });

  api.registerTool({
    name: "tradingops_risk_reset_halt",
    description: "Reset risk halt / circuit breaker state.",
    optional: true,
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "tradingops_risk_reset_halt", "/risk/reset_halt", "POST", {}),
  });

  api.registerTool({
    name: "tradingops_manual_signal",
    description: "Submit a manual signal through the Ops API. Requires OPS_ALLOW_MANUAL_SIGNAL=true on server side.",
    optional: true,
    inputSchema: Type.Object({
      symbol: Type.String(),
      signal_type: Type.String(),
      strength: Type.Optional(Type.Number({ minimum: 0, maximum: 1 })),
      reason: Type.Optional(Type.String()),
    }),
    execute: async (input: any) => callTool(api, "tradingops_manual_signal", "/trading/submit_manual_signal", "POST", input || {}),
  });

  api.registerTool({
    name: "polymarket_status",
    description: "Read Polymarket worker, subscriptions, alerts, and quote status from Ops API.",
    inputSchema: Type.Object({}),
    execute: async () => callTool(api, "polymarket_status", "/polymarket/status", "GET"),
  });

  api.registerTool({
    name: "polymarket_subscribe",
    description: "Refresh or manually adjust Polymarket subscriptions for a category.",
    optional: true,
    inputSchema: Type.Object({
      category: Type.String(),
      mode: Type.Optional(Type.String()),
      keywords: Type.Optional(Type.Array(Type.String())),
      tags: Type.Optional(Type.Array(Type.Number())),
      max_markets: Type.Optional(Type.Number({ minimum: 1, maximum: 100 })),
    }),
    execute: async (input: any) => callTool(api, "polymarket_subscribe", "/polymarket/subscribe", "POST", input || {}),
  });

  api.registerTool({
    name: "polymarket_alerts",
    description: "Fetch recent Polymarket shock alerts.",
    inputSchema: Type.Object({
      since: Type.Optional(Type.String()),
      category: Type.Optional(Type.String()),
      limit: Type.Optional(Type.Number({ minimum: 1, maximum: 1000 })),
    }),
    execute: async (input: any) => {
      const params = new URLSearchParams();
      if (input?.since) params.set("since", String(input.since));
      if (input?.category) params.set("category", String(input.category));
      if (input?.limit) params.set("limit", String(input.limit));
      const query = params.toString();
      return callTool(api, "polymarket_alerts", `/polymarket/alerts${query ? `?${query}` : ""}`, "GET");
    },
  });

  api.registerTool({
    name: "polymarket_features",
    description: "Fetch Polymarket-derived features for a symbol/timeframe.",
    inputSchema: Type.Object({
      symbol: Type.String(),
      tf: Type.Optional(Type.String()),
      since: Type.Optional(Type.String()),
    }),
    execute: async (input: any) => {
      const params = new URLSearchParams({ symbol: String(input?.symbol || "") });
      if (input?.tf) params.set("tf", String(input.tf));
      if (input?.since) params.set("since", String(input.since));
      return callTool(api, "polymarket_features", `/polymarket/features?${params.toString()}`, "GET");
    },
  });
}
