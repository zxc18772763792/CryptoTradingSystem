(function () {
    const API = "/api/news";
    const SUMMARY_STALE_MS = 90 * 1000;
    const SUMMARY_LIMIT = 24;
    const UI_TIMEZONE = (typeof window !== "undefined" && window.CTS_UI_TIMEZONE) || "Asia/Shanghai";
    const state = {
        timer: null,
        bucketRenderTimer: null,
        latest: null,
        brief: null,
        summary: null,
        coverage: null,
        summaryLoading: false,
        health: null,
        worker: null,
        pulling: false,
        refreshPromise: null,
        summarizePromise: null,
        llmKickoffPromise: null,
        summaryLoadedAt: 0,
        needsRefresh: false,
    };

    const el = (id) => document.getElementById(id);
    const getVal = (id, def = "") => String(el(id)?.value || def);
    const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
    const plainText = (v) => String(v ?? "").replace(/<\s*br\s*\/?>/gi, " ").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

    function parseTs(v) {
        if (!v) return null;
        let text = String(v).trim();
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(text)) text += "Z";
        const d = new Date(text);
        return Number.isFinite(d.getTime()) ? d : null;
    }

    function fmtTs(v) {
        const d = parseTs(v);
        return d ? d.toLocaleString("zh-CN", { hour12: false, timeZone: UI_TIMEZONE }) : "--";
    }

    function isStandalonePage() {
        return document.body?.classList.contains("news-page") || location.pathname === "/news";
    }

    function isNewsVisible() {
        const feed = el("news-unstructured-list");
        if (!feed) return false;
        if (isStandalonePage()) return true;
        const tab = el("news");
        return !!tab && tab.classList.contains("active");
    }

    function sentimentClass(v) {
        if (Number(v) > 0) return "news-sentiment-pos";
        if (Number(v) < 0) return "news-sentiment-neg";
        return "news-sentiment-neu";
    }

    function structuredSentimentText(v) {
        if (Number(v) > 0) return "利好";
        if (Number(v) < 0) return "利空";
        return "中性";
    }

    function summarySentimentText(v) {
        const key = String(v || "neutral").toLowerCase();
        if (key === "positive") return "利好";
        if (key === "negative") return "利空";
        return "中性";
    }

    function summarySourceText(v) {
        const key = String(v || "").trim().toLowerCase();
        if (!key || key === "not_summarized") return "未摘要";
        if (key === "stored") return "已入库摘要";
        if (key.includes("openai") || key.includes("responses") || key.includes("codex") || key.startsWith("gpt")) {
            return "GPT摘要";
        }
        if (key.includes("fallback")) return "规则回退";
        if (key.startsWith("llm")) return "LLM摘要";
        return plainText(key).slice(0, 20) || "未知来源";
    }

    function processingStatusText(v) {
        const key = String(v || "").toLowerCase();
        if (key === "pending") return "待摘要/抽取";
        if (key === "running") return "AI处理中";
        if (key === "retry") return "待补跑";
        if (key === "failed") return "本轮失败";
        if (key === "done_no_event") return "已抽取无事件";
        if (key === "summarized_no_event") return "已补中文摘要";
        if (key === "skipped_low_importance") return "低优先跳过";
        if (key === "not_queued") return "尚未入队";
        if (key === "structured_event") return "已结构化";
        return "状态未知";
    }

    function processingStatusHint(v) {
        const key = String(v || "").toLowerCase();
        if (key === "pending") return "已进入待处理队列，等待摘要或事件抽取。";
        if (key === "running") return "AI 正在处理这条新闻。";
        if (key === "retry") return "这条新闻会在后续轮次继续补跑。";
        if (key === "failed") return "本轮抽取失败，后台会按节奏温和补跑。";
        if (key === "done_no_event") return "AI 已读完，但未识别出可入库的结构化交易事件。";
        if (key === "summarized_no_event") return "中文摘要已补好，后续仍可继续尝试结构化。";
        if (key === "skipped_low_importance") return "优先级较低，当前未送入 AI 队列。";
        if (key === "not_queued") return "尚未进入 AI 队列。";
        if (key === "structured_event") return "已生成结构化事件，可直接用于后续分析。";
        return "暂无状态说明。";
    }

    function requeueReasonText(v) {
        const key = String(v || "").toLowerCase();
        if (key === "requeued") return "已温和重排队";
        if (key === "queue_busy") return "有新任务优先";
        if (key === "cooldown") return "补跑冷却中";
        if (key === "no_failed") return "没有失败历史";
        if (key === "no_candidates") return "没有可补跑候选";
        if (key === "disabled") return "补跑已关闭";
        if (key === "llm_disabled") return "LLM未启用";
        return key || "--";
    }

    function notify(msg, isError = false) {
        if (typeof window.notify === "function") {
            window.notify(msg, isError);
            return;
        }
        const box = el("notification");
        if (!box) return;
        box.textContent = String(msg || "");
        box.className = `notification show ${isError ? "error" : ""}`;
        setTimeout(() => box.classList.remove("show"), 2600);
    }

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timeoutMs = Math.max(3000, Number(options.timeoutMs || 15000));
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const sep = path.includes("?") ? "&" : "?";
        try {
            const res = await fetch(`${API}${path}${sep}_ts=${Date.now()}`, {
                ...options,
                signal: controller.signal,
                cache: "no-store",
                headers: {
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                    Pragma: "no-cache",
                    ...(options.headers || {}),
                },
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.detail || data.error || `请求失败(${res.status})`);
            return data;
        } catch (err) {
            if (err?.name === "AbortError") throw new Error(`请求超时(${timeoutMs}ms): ${path}`);
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    function params(extra = {}) {
        const p = new URLSearchParams(extra);
        const symbol = getVal("news-symbol").trim().toUpperCase();
        if (symbol) p.set("symbol", symbol);
        return p;
    }

    function feedStats(items) {
        const rows = Array.isArray(items) ? items : [];
        const out = {
            total: rows.length,
            structured: 0,
            unstructured: 0,
            sentiment: { positive: 0, neutral: 0, negative: 0 },
            unstructured_breakdown: {
                pending: 0,
                running: 0,
                retry: 0,
                failed: 0,
                done_no_event: 0,
                summarized_no_event: 0,
                skipped_low_importance: 0,
                not_queued: 0,
                unknown_unstructured: 0,
            },
        };
        for (const item of rows) {
            if (item?.has_event) {
                out.structured += 1;
                const s = Number(item.sentiment || 0);
                if (s > 0) out.sentiment.positive += 1;
                else if (s < 0) out.sentiment.negative += 1;
                else out.sentiment.neutral += 1;
                continue;
            }
            out.unstructured += 1;
            const status = String(item?.processing_status || "unknown_unstructured").toLowerCase();
            out.unstructured_breakdown[status] = Number(out.unstructured_breakdown[status] || 0) + 1;
            const s = String(item?.summary_sentiment || "neutral").toLowerCase();
            if (s === "positive") out.sentiment.positive += 1;
            else if (s === "negative") out.sentiment.negative += 1;
            else out.sentiment.neutral += 1;
        }
        return out;
    }

    function summaryMetrics() {
        const gran = getVal("news-summary-granularity", "feed");
        if (gran === "feed" || !state.summary) {
            return {
                scope: "当前列表",
                ...feedStats(state.latest?.items || []),
            };
        }
        const buckets = getBucketRows(gran);
        const latestBucket = buckets.length ? buckets[buckets.length - 1] : null;
        if (!latestBucket) {
            return {
                scope: `最近 ${gran} 桶`,
                total: 0,
                structured: 0,
                unstructured: 0,
                sentiment: { positive: 0, neutral: 0, negative: 0 },
            };
        }
        return {
            scope: `最近 ${gran} 桶 ${fmtTs(latestBucket.bucket_start)}`,
            total: Number(latestBucket.count || 0),
            structured: Number(latestBucket.count || 0),
            unstructured: 0,
            sentiment: {
                positive: Number(latestBucket.positive || 0),
                neutral: Number(latestBucket.neutral || 0),
                negative: Number(latestBucket.negative || 0),
            },
        };
    }

    function normalizeBuckets(raw) {
        if (Array.isArray(raw)) return raw;
        if (!raw || typeof raw !== "object") return [];
        if (Array.isArray(raw.items)) return raw.items;
        if (Array.isArray(raw.buckets)) return raw.buckets;
        const rows = Object.entries(raw).map(([bucketStart, row]) => ({
            ...(row && typeof row === "object" ? row : {}),
            bucket_start: (row && row.bucket_start) || bucketStart,
            count: Number(row?.count || 0),
            positive: Number(row?.positive || 0),
            neutral: Number(row?.neutral || 0),
            negative: Number(row?.negative || 0),
        }));
        rows.sort((a, b) => {
            const ta = parseTs(a.bucket_start)?.getTime() || 0;
            const tb = parseTs(b.bucket_start)?.getTime() || 0;
            return ta - tb;
        });
        return rows;
    }

    function getBucketRows(gran) {
        return normalizeBuckets(state.summary?.bucket_stats?.[gran]);
    }

    function scheduleBucketRender(delayMs = 220) {
        if (state.bucketRenderTimer) clearTimeout(state.bucketRenderTimer);
        state.bucketRenderTimer = setTimeout(() => {
            state.bucketRenderTimer = null;
            renderBuckets();
        }, Math.max(0, Number(delayMs) || 0));
    }

    function renderHealth() {
        const badge = el("news-health-badge");
        if (!badge) return;
        const sourceStates = state.health?.source_states || state.worker?.source_states || state.brief?.source_states || [];
        const queue = state.health?.llm_queue || state.worker?.llm_queue || state.brief?.llm_queue || {};
        const enabled = Object.entries(state.health?.sources || {}).filter(([, v]) => !!v).length || sourceStates.length;
        const errors = sourceStates.filter((row) => Number(row?.error_count || 0) > 0).length;
        const failed = Number(queue?.counts?.failed || 0);
        badge.className = `status-badge ${(errors || failed) ? "warning" : "connected"}`;
        badge.textContent = `来源 ${enabled} | 待处理 ${Number(queue?.pending_total || 0)} | 失败待修 ${failed} | 异常源 ${errors}`;
        if (el("news-now-time")) {
            el("news-now-time").textContent = fmtTs(state.health?.timestamp || state.worker?.timestamp || new Date().toISOString());
        }
    }

    function renderQueue() {
        const queue = state.health?.llm_queue || state.worker?.llm_queue || state.brief?.llm_queue || {};
        const counts = queue?.counts || {};
        const pending = Number(queue?.pending_total || 0);
        const running = Number(counts?.running || 0);
        const done = Number(counts?.done || 0);
        const retry = Number(counts?.retry || 0);
        const failed = Number(counts?.failed || 0);
        const mapping = [
            ["news-llm-pending-count", pending],
            ["news-llm-running-count", running],
            ["news-llm-done-count", done],
            ["news-llm-retry-count", retry + failed],
        ];
        mapping.forEach(([id, value]) => {
            if (el(id)) el(id).textContent = String(value);
        });
        const retryMetricLabel = el("news-llm-retry-count")?.parentElement?.querySelector?.(".label");
        if (retryMetricLabel) retryMetricLabel.textContent = "异常待修补";
        if (!el("news-llm-queue-note")) return;
        const lastPull = state.worker?.last_pull || {};
        const lastLlm = state.worker?.last_llm_batch || state.worker?.manual_llm_job?.latest_result || {};
        const failedRequeue = lastLlm?.failed_requeue || {};
        const retryResult = lastLlm?.retry_result || {};
        const summaryRepair = lastLlm?.summary_repair || {};
        const requeueCount = Number(failedRequeue?.requeued_count || 0);
        const repairUpdates = Number(summaryRepair?.updated_raw_count || 0) + Number(summaryRepair?.updated_event_count || 0);
        const backgroundEnabled = Boolean(state.worker?.background_llm_enabled);
        const manualActive = Boolean(state.worker?.manual_llm_job?.active_job_id);
        const llmMode = backgroundEnabled
            ? "后台自动修补"
            : manualActive
                ? "页面手动补跑"
                : pending > 0 || failed > 0
                    ? "等待页面手动触发"
                    : "后台未开启";
        let queuePhase = "队列空闲";
        if (queue?.backoff_until) queuePhase = "全局退避中";
        else if (running > 0) queuePhase = "AI处理中";
        else if (pending > 0) queuePhase = backgroundEnabled ? "新任务待处理" : "新任务待手动触发";
        else if (failed > 0) queuePhase = backgroundEnabled ? "温和修补失败历史" : "失败历史待手动补跑";
        const repairPaceText = lastLlm?.timestamp
            ? `${fmtTs(lastLlm.timestamp)} | ${requeueReasonText(failedRequeue?.reason)} | 重排 ${requeueCount} | 立即补跑 ${Number(retryResult?.claimed || 0)}`
            : "--";
        const summaryRepairText = lastLlm?.timestamp
            ? `${fmtTs(lastLlm.timestamp)} | 原始 ${Number(summaryRepair?.updated_raw_count || 0)} | 事件 ${Number(summaryRepair?.updated_event_count || 0)}`
            : "--";
        el("news-llm-queue-note").innerHTML = [
            `<div class="list-item"><span>LLM 模式</span><span>${esc(llmMode)}</span></div>`,
            `<div class="list-item"><span>处理阶段</span><span>${esc(queuePhase)}</span></div>`,
            `<div class="list-item"><span>新任务待处理 / 失败待修补</span><span>${pending} / ${failed}</span></div>`,
            `<div class="list-item"><span>重试中 / 已完成</span><span>${retry} / ${done}</span></div>`,
            `<div class="list-item"><span>失败历史清理节奏</span><span>${esc(repairPaceText)}</span></div>`,
            `<div class="list-item"><span>最近摘要修补</span><span>${esc(summaryRepairText)}</span></div>`,
            `<div class="list-item"><span>最高优先级 / 本轮修补数</span><span>${Number(queue?.max_priority || 0)} / ${repairUpdates}</span></div>`,
            `<div class="list-item"><span>下次重试 / 全局退避</span><span>${fmtTs(queue?.next_retry_at)} / ${fmtTs(queue?.backoff_until)}</span></div>`,
            `<div class="list-item"><span>最近拉取 / LLM</span><span>${fmtTs(lastPull?.timestamp)} / ${fmtTs(lastLlm?.timestamp)}</span></div>`,
        ].join("");
    }

    function renderProviders() {
        const box = el("news-source-stats");
        if (!box) return;
        const sourceSummary = state.summary?.source_summary || {};
        const byProvider = state.summary?.by_provider || state.brief?.by_provider || {};
        const bySource = state.latest?.source_stats?.by_source || {};
        const names = Array.from(new Set([...Object.keys(sourceSummary), ...Object.keys(byProvider), ...Object.keys(bySource)])).filter(Boolean);
        if (!names.length) {
            box.innerHTML = '<div class="list-item">暂无来源统计</div>';
            return;
        }
        names.sort((a, b) => Number(sourceSummary[b]?.inserted_count || byProvider[b] || bySource[b] || 0) - Number(sourceSummary[a]?.inserted_count || byProvider[a] || bySource[a] || 0));
        box.innerHTML = names.slice(0, 12).map((name) => {
            const inserted = Number(sourceSummary[name]?.inserted_count || byProvider[name] || 0);
            const live = Number(bySource[name] || 0);
            const lastErr = plainText(sourceSummary[name]?.last_error || "");
            const tail = lastErr ? ` | 最近错误 ${esc(lastErr.slice(0, 32))}` : "";
            return `<div class="list-item"><span>${esc(plainText(name))}</span><span>24h ${inserted} | 当前 ${live}${tail}</span></div>`;
        }).join("");
    }

    function renderArchiveCoverage() {
        const box = el("news-archive-note");
        if (!box) return;
        const coverage = state.coverage || {};
        const contract = coverage?.archive_contract || {};
        const topSources = Array.isArray(coverage?.top_sources) ? coverage.top_sources.slice(0, 3) : [];
        const topText = topSources.length
            ? topSources.map((row) => `${plainText(row.source || "-")}:${Number(row.count || 0)}`).join(" | ")
            : "--";
        box.innerHTML = [
            `<div class="list-item"><span>历史原文总量 / 活跃源(7d)</span><span>${Number(coverage?.total_count || 0)} / ${Number(coverage?.active_sources_7d || 0)}</span></div>`,
            `<div class="list-item"><span>历史跨度 / 最近入库</span><span>${Number(coverage?.history_span_days || 0).toFixed(1)} 天 / ${fmtTs(coverage?.latest_fetched_at)}</span></div>`,
            `<div class="list-item"><span>最早新闻 / 最新新闻</span><span>${fmtTs(coverage?.earliest_published_at)} / ${fmtTs(coverage?.latest_published_at)}</span></div>`,
            `<div class="list-item"><span>近24h / 7d / 30d</span><span>${Number(coverage?.count_24h || 0)} / ${Number(coverage?.count_7d || 0)} / ${Number(coverage?.count_30d || 0)}</span></div>`,
            `<div class="list-item"><span>最近写入模式样本</span><span>${esc(Object.entries(coverage?.recent_ingest_mode_counts || {}).map(([name, count]) => `${name}:${count}`).join(" | ") || "--")}</span></div>`,
            `<div class="list-item"><span>样本Top源</span><span>${esc(topText)}</span></div>`,
            `<div class="list-item"><span>归档说明</span><span>${esc(contract?.guarantees_full_upstream_history ? "已保证全历史" : "保存已拉到原文，可额外补拉历史")}</span></div>`,
        ].join("");
    }

    function renderSummary() {
        const latest = state.latest || {};
        const base = state.summary || state.brief || {};
        const latestRawAt = base?.latest_raw_at || state.brief?.latest_raw_at || null;
        const latestEventAt = base?.latest_event_at || state.brief?.latest_event_at || null;
        const stats = summaryMetrics();
        const breakdown = latest?.feed_stats?.unstructured_breakdown || stats?.unstructured_breakdown || {};
        if (el("news-events-count")) el("news-events-count").textContent = String(stats.total);
        if (el("news-positive-count")) el("news-positive-count").textContent = String(stats.sentiment.positive);
        if (el("news-neutral-count")) el("news-neutral-count").textContent = String(stats.sentiment.neutral);
        if (el("news-negative-count")) el("news-negative-count").textContent = String(stats.sentiment.negative);
        if (el("news-summary-gran-meta")) el("news-summary-gran-meta").textContent = `${stats.scope} | ${stats.total} 条`;
        if (!el("news-summary-note")) return;
        const summaryState = (latest?.fallback_reason || state.summary?.fallback_reason) ? "摘要接口较慢，已回退快速列表" : "GPT 摘要正常";
        el("news-summary-note").innerHTML = [
            `<div class="list-item"><span>24h 原始新闻 / 事件</span><span>${Number(base?.raw_count || 0)} / ${Number(base?.events_count || 0)}</span></div>`,
            `<div class="list-item"><span>当前 Feed 结构化 / 未结构化</span><span>${Number(stats.structured || 0)} / ${Number(stats.unstructured || 0)}</span></div>`,
            `<div class="list-item"><span>待摘要 / AI处理中</span><span>${Number(breakdown.pending || 0)} / ${Number(breakdown.running || 0)}</span></div>`,
            `<div class="list-item"><span>待补跑 / 本轮失败</span><span>${Number(breakdown.retry || 0)} / ${Number(breakdown.failed || 0)}</span></div>`,
            `<div class="list-item"><span>已补中文摘要 / 已抽取无事件</span><span>${Number(breakdown.summarized_no_event || 0)} / ${Number(breakdown.done_no_event || 0)}</span></div>`,
            `<div class="list-item"><span>低优先跳过 / 尚未入队</span><span>${Number(breakdown.skipped_low_importance || 0)} / ${Number(breakdown.not_queued || 0)}</span></div>`,
            `<div class="list-item"><span>最近原始新闻 / 事件</span><span>${fmtTs(latestRawAt)} / ${fmtTs(latestEventAt)}</span></div>`,
            `<div class="list-item"><span>标题摘要状态</span><span>${esc(summaryState)}</span></div>`,
        ].join("");
    }

    function renderBuckets() {
        const chart = el("news-bucket-chart");
        const list = el("news-bucket-list");
        const meta = el("news-bucket-meta");
        const gran = getVal("news-bucket-granularity", "1h");
        const buckets = getBucketRows(gran);
        if (chart && isNewsVisible() && ((chart.clientWidth || 0) < 64 || (chart.clientHeight || 0) < 64)) {
            if (meta) meta.textContent = "结构化统计面板切换中，正在重绘...";
            scheduleBucketRender(260);
            return;
        }
        if (meta) {
            if (buckets.length) {
                meta.textContent = `结构化事件统计（${gran}）| 桶数 ${buckets.length}`;
            } else if (state.summary?.fallback_reason) {
                meta.textContent = `结构化统计降级：${state.summary.fallback_reason}`;
            } else if (state.summary) {
                meta.textContent = "结构化统计暂无数据";
            } else {
                meta.textContent = "结构化统计暂不可用，自动重试中...";
            }
        }
        if (!buckets.length) {
            const emptyText = state.summary ? "暂无结构化事件统计" : "结构化统计暂不可用，正在自动重试";
            if (chart) {
                if (typeof Plotly !== "undefined") {
                    try { Plotly.purge(chart); } catch (_) {}
                }
                chart.innerHTML = `<div class="list-item news-bucket-fallback">${emptyText}</div>`;
            }
            if (list) list.innerHTML = `<div class="list-item">${emptyText}</div>`;
            return;
        }
        const recent = buckets.slice(-36);
        if (chart && typeof Plotly !== "undefined") {
            try {
                chart.querySelectorAll(".news-bucket-fallback").forEach((node) => node.remove());
                if (!chart.classList.contains("js-plotly-plot")) {
                    chart.innerHTML = "";
                }
                if (chart.classList.contains("js-plotly-plot") && !chart.querySelector(".plot-container")) {
                    try { Plotly.purge(chart); } catch (_) {}
                    chart.innerHTML = "";
                }
                Plotly.react(chart, [
                    { type: "bar", x: recent.map((x) => parseTs(x.bucket_start) || x.bucket_start), y: recent.map((x) => Number(x.count || 0)), name: "总数", marker: { color: "#1f9d63", opacity: 0.35 } },
                    { type: "scatter", mode: "lines+markers", x: recent.map((x) => parseTs(x.bucket_start) || x.bucket_start), y: recent.map((x) => Number(x.positive || 0)), name: "利好", line: { color: "#20bf78", width: 2 } },
                    { type: "scatter", mode: "lines+markers", x: recent.map((x) => parseTs(x.bucket_start) || x.bucket_start), y: recent.map((x) => Number(x.negative || 0)), name: "利空", line: { color: "#ea5b61", width: 2 } },
                ], {
                    paper_bgcolor: "#111723",
                    plot_bgcolor: "#111723",
                    font: { color: "#d7dde8" },
                    margin: { l: 36, r: 24, t: 16, b: 32 },
                    legend: { orientation: "h", y: 1.12 },
                    barmode: "overlay",
                    hovermode: "x unified",
                }, { responsive: true, displaylogo: false });
                setTimeout(() => {
                    try { Plotly.Plots.resize(chart); } catch (_) {}
                }, 90);
            } catch (err) {
                try { Plotly.purge(chart); } catch (_) {}
                chart.innerHTML = `<div class="list-item">结构化统计图渲染失败: ${esc(err?.message || "unknown")}</div>`;
                if (meta) meta.textContent = "结构化统计图渲染失败，已降级为列表";
            }
        } else if (chart) {
            chart.innerHTML = '<div class="list-item">图表库未加载，无法绘制结构化统计图</div>';
            if (typeof Plotly === "undefined") scheduleBucketRender(300);
        }
        if (list) {
            list.innerHTML = recent.slice().reverse().map((row) => {
                const total = Number(row.count || 0);
                const positive = Number(row.positive || 0);
                const neutral = Number(row.neutral || 0);
                const negative = Number(row.negative || 0);
                return `<div class="list-item"><span>${fmtTs(row.bucket_start)}</span><span>总 ${total} | +${positive} / 0:${neutral} / -${negative}</span></div>`;
            }).join("");
        }
    }

    function renderFeed() {
        const items = state.latest?.items || [];
        const rawBox = el("news-unstructured-list");
        const structuredBox = el("news-structured-list");
        const raws = items.filter((x) => !x.has_event);
        const structs = items.filter((x) => x.has_event);
        if (el("news-unstructured-count")) el("news-unstructured-count").textContent = `${raws.length} 条`;
        if (el("news-structured-count")) el("news-structured-count").textContent = `${structs.length} 条`;
        if (rawBox) {
            rawBox.innerHTML = raws.length ? raws.map((item) => {
                const title = esc(plainText(item.summary_title || item.title || "（无标题）"));
                const titleHtml = item.url ? `<a class="news-title news-white" href="${esc(item.url)}" target="_blank" rel="noopener noreferrer">${title}</a>` : `<span class="news-title news-white">${title}</span>`;
                const sourceText = summarySourceText(item.summary_source);
                const statusText = processingStatusText(item.processing_status);
                const statusHint = processingStatusHint(item.processing_status);
                return `<div class="list-item news-row"><div class="news-main">${titleHtml}<div class="news-meta"><span>${fmtTs(item.published_at)}</span><span class="news-tag-white">${esc(plainText(item.provider || "-"))}</span><span class="${sentimentClass(item.summary_sentiment === "positive" ? 1 : item.summary_sentiment === "negative" ? -1 : 0)}">${summarySentimentText(item.summary_sentiment)}</span><span class="news-tag-white" title="${esc(`摘要来源: ${sourceText}`)}">摘要:${esc(sourceText)}</span><span class="news-tag-white" title="${esc(statusHint)}">${esc(statusText)}</span></div></div></div>`;
            }).join("") : '<div class="list-item news-white">暂无未结构化新闻</div>';
        }
        if (structuredBox) {
            structuredBox.innerHTML = structs.length ? structs.map((item) => {
                const title = esc(plainText(item.summary_title || item.title || "（无标题）"));
                const symbolText = (Array.isArray(item.related_symbols) && item.related_symbols.length ? item.related_symbols : [item.symbol]).filter(Boolean).join(" / ");
                const providerText = (Array.isArray(item.related_providers) && item.related_providers.length ? item.related_providers : [item.provider]).filter(Boolean).join(" / ");
                const typeText = (Array.isArray(item.related_event_types) && item.related_event_types.length ? item.related_event_types : [item.event_type]).filter(Boolean).join(" / ");
                const groupTag = Number(item.event_count || 1) > 1 ? `<span class="news-tag-white">关联 ${Number(item.event_count || 1)} 个事件</span>` : "";
                const titleHtml = item.url ? `<a class="news-title news-white" href="${esc(item.url)}" target="_blank" rel="noopener noreferrer">${title}</a>` : `<span class="news-title news-white">${title}</span>`;
                return `<div class="list-item news-row"><div class="news-main">${titleHtml}<div class="news-meta"><span>${fmtTs(item.published_at)}</span><span class="news-tag-white">${esc(plainText(providerText || "-"))}</span><span class="news-tag-white">${esc(plainText(symbolText || "-"))}</span><span class="news-tag-white">${esc(plainText(typeText || "raw"))}</span><span class="${sentimentClass(item.sentiment)}">${structuredSentimentText(item.sentiment)}</span><span>影响 ${Number(item.impact_score || 0).toFixed(3)}</span>${groupTag}<span>${esc(plainText(item.source || "-"))}</span></div></div></div>`;
            }).join("") : '<div class="list-item news-white">暂无已结构化事件</div>';
        }
    }

    function renderAll() {
        renderHealth();
        renderQueue();
        renderProviders();
        renderArchiveCoverage();
        renderSummary();
        renderBuckets();
        renderFeed();
    }

    async function loadBrief() {
        return request(`/brief?${params({ hours: getVal("news-hours", "24"), feed_limit: String(Math.min(60, Number(getVal("news-max-records", "120")) || 120)) }).toString()}`, { timeoutMs: 15000 });
    }

    async function loadLatestFast() {
        return request(`/latest?${params({ hours: getVal("news-hours", "24"), limit: getVal("news-max-records", "120"), summarize: "false" }).toString()}`, { timeoutMs: 25000 });
    }

    async function loadLatestSummarized() {
        return request(`/latest?${params({ hours: getVal("news-hours", "24"), limit: String(Math.min(SUMMARY_LIMIT, Number(getVal("news-max-records", "120")) || SUMMARY_LIMIT)), summarize: "true" }).toString()}`, { timeoutMs: 25000 });
    }

    async function loadSummary() {
        return request(`/summary?${params({ hours: getVal("news-hours", "24"), feed_limit: String(Math.min(80, Number(getVal("news-max-records", "120")) || 120)) }).toString()}`, { timeoutMs: 18000 });
    }

    async function loadCoverage() {
        return request("/raw/coverage", { timeoutMs: 15000 });
    }

    function mergeSummaries(summaryFeed) {
        const incoming = Array.isArray(summaryFeed?.items) ? summaryFeed.items : [];
        if (!state.latest?.items?.length || !incoming.length) return;
        const map = new Map(incoming.map((item) => [String(item.id || ""), item]));
        let changed = false;
        state.latest.items = state.latest.items.map((item) => {
            const match = map.get(String(item.id || ""));
            if (!match) return item;
            changed = true;
            return {
                ...item,
                summary_title: match.summary_title || item.summary_title || item.title,
                summary_sentiment: match.summary_sentiment || item.summary_sentiment || "neutral",
                summary_source: match.summary_source || item.summary_source || null,
            };
        });
        if (changed) {
            renderSummary();
            renderFeed();
        }
    }

    async function enrichHeadlines() {
        if (!isNewsVisible() || state.summarizePromise || !state.latest?.items?.length) return;
        state.summarizePromise = loadLatestSummarized()
            .then((payload) => mergeSummaries(payload))
            .catch(() => {})
            .finally(() => {
                state.summarizePromise = null;
            });
        return state.summarizePromise;
    }

    async function ensureLlmKickoff() {
        if (!isNewsVisible() || state.llmKickoffPromise) return null;
        const worker = state.worker || {};
        const queue = state.health?.llm_queue || state.worker?.llm_queue || state.brief?.llm_queue || {};
        const pending = Number(queue?.pending_total || 0);
        const running = Number(queue?.counts?.running || 0);
        const activeJobId = String(worker?.manual_llm_job?.active_job_id || "").trim();
        if (!pending || running > 0 || activeJobId || worker?.background_llm_enabled) return null;
        state.llmKickoffPromise = request("/worker/run_once?llm_limit=8&background=true", { method: "POST", timeoutMs: 6000 })
            .then((result) => {
                state.worker = {
                    ...(state.worker || {}),
                    manual_llm_job: {
                        ...((state.worker || {}).manual_llm_job || {}),
                        active_job_id: result?.job_id || null,
                        latest_result: ((state.worker || {}).manual_llm_job || {}).latest_result || null,
                    },
                };
                renderQueue();
                return result;
            })
            .catch(() => null)
            .finally(() => {
                state.llmKickoffPromise = null;
            });
        return state.llmKickoffPromise;
    }

    async function refreshAll(forceSummary = false) {
        if (!isNewsVisible()) {
            state.needsRefresh = true;
            return null;
        }
        if (state.refreshPromise) {
            state.needsRefresh = true;
            return state.refreshPromise;
        }
        const shouldLoadSummary = forceSummary || !state.summary || (Date.now() - state.summaryLoadedAt) >= SUMMARY_STALE_MS;
        state.needsRefresh = false;
        state.refreshPromise = (async () => {
            try {
                const [briefRes, latestRes, healthRes, workerRes, coverageRes] = await Promise.allSettled([
                    loadBrief(),
                    loadLatestFast(),
                    request("/health", { timeoutMs: 15000 }).catch(() => null),
                    request("/worker_status", { timeoutMs: 15000 }).catch(() => null),
                    loadCoverage().catch(() => null),
                ]);
                if (briefRes.status === "fulfilled") state.brief = briefRes.value || state.brief || null;
                if (latestRes.status === "fulfilled") state.latest = latestRes.value || state.latest || null;
                if (healthRes.status === "fulfilled") state.health = healthRes.value || state.health || null;
                if (workerRes.status === "fulfilled") state.worker = workerRes.value || state.worker || null;
                if (coverageRes.status === "fulfilled") state.coverage = coverageRes.value || state.coverage || null;
                if (!state.latest) {
                    const latestErr = latestRes.status === "rejected" ? latestRes.reason : new Error("新闻列表不可用");
                    throw latestErr;
                }
                renderAll();
                ensureLlmKickoff().catch(() => {});
                if (shouldLoadSummary) {
                    state.summaryLoading = true;
                    loadSummary().then((summary) => {
                        state.summary = summary || null;
                        state.summaryLoadedAt = Date.now();
                        renderAll();
                    }).catch(() => {
                        // Keep the page responsive when summary endpoint is slow/failing.
                        state.summary = state.summary || null;
                        renderBuckets();
                        renderProviders();
                    }).finally(() => {
                        state.summaryLoading = false;
                    });
                }
                enrichHeadlines();
            } catch (err) {
                notify(`新闻刷新失败: ${err.message}`, true);
                const rawBox = el("news-unstructured-list");
                const structuredBox = el("news-structured-list");
                if (rawBox && !state.latest) rawBox.innerHTML = `<div class="list-item news-white">加载失败: ${esc(err.message)}</div>`;
                if (structuredBox && !state.latest) structuredBox.innerHTML = `<div class="list-item news-white">加载失败: ${esc(err.message)}</div>`;
            } finally {
                state.refreshPromise = null;
                if (state.needsRefresh && isNewsVisible()) {
                    state.needsRefresh = false;
                    setTimeout(() => refreshAll(false), 80);
                }
            }
        })();
        return state.refreshPromise;
    }

    async function waitJob(jobId) {
        const started = Date.now();
        while (Date.now() - started < 10 * 60 * 1000) {
            const status = await request("/pull_status", { timeoutMs: 15000 });
            const jobs = Array.isArray(status?.jobs) ? status.jobs : [];
            const job = jobs.find((row) => String(row?.job_id || "") === String(jobId || ""));
            if (job?.status === "completed") return job;
            if (job?.status === "failed") throw new Error(job?.error || "新闻拉取失败");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
        throw new Error(`新闻后台任务超时: ${jobId}`);
    }

    async function waitLlmJob(jobId) {
        const started = Date.now();
        while (Date.now() - started < 10 * 60 * 1000) {
            const status = await request("/pull_status", { timeoutMs: 15000 });
            const jobs = Array.isArray(status?.llm_jobs) ? status.llm_jobs : [];
            const job = jobs.find((row) => String(row?.job_id || "") === String(jobId || ""));
            if (job?.status === "completed") return job;
            if (job?.status === "failed") throw new Error(job?.error || "LLM 任务失败");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
        throw new Error(`LLM 后台任务超时: ${jobId}`);
    }

    async function pullNow() {
        if (state.pulling) return;
        state.pulling = true;
        try {
            const result = await request("/pull_now?background=true", {
                method: "POST",
                body: JSON.stringify({
                    since_minutes: Math.max(30, Math.min(1440, (Number(getVal("news-hours", "24")) || 24) * 60)),
                    max_records: Math.max(20, Math.min(250, Number(getVal("news-max-records", "120")) || 120)),
                }),
                timeoutMs: 20000,
            });
            if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(result, null, 2);
            let finalResult = result;
            if (result?.queued && result?.job_id) {
                notify(`新闻任务已排队: ${result.job_id}`);
                finalResult = (await waitJob(result.job_id))?.result || result;
                if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(finalResult, null, 2);
            }
            if (Number(finalResult?.queued_count || 0) > 0) {
                request("/worker/run_once?llm_limit=8&background=true", { method: "POST", timeoutMs: 6000 }).catch(() => ({}));
            }
            notify(`拉取完成: 原始 ${Number(finalResult?.raw_inserted_count || 0)} 条, 结构化 ${Number(finalResult?.events_count || 0)} 条`);
            await refreshAll(true);
        } catch (err) {
            if (el("news-action-output")) el("news-action-output").textContent = `拉取失败: ${err.message}`;
            notify(`拉取失败: ${err.message}`, true);
        } finally {
            state.pulling = false;
        }
    }

    async function backfillArchiveNow() {
        if (state.pulling) return;
        state.pulling = true;
        try {
            const result = await request("/ingest/backfill_history", {
                method: "POST",
                body: JSON.stringify({
                    hours: Math.max(24, Math.min(24 * 30, Number(getVal("news-hours", "24")) || 24)),
                    max_records: Math.max(120, Math.min(800, Number(getVal("news-max-records", "120")) || 120)),
                    source_names: [],
                    enqueue_llm: false,
                }),
                timeoutMs: 90000,
            });
            if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(result, null, 2);
            notify(`历史原文补拉完成: 新增 ${Number(result?.raw_inserted_count || 0)} 条，当前总库 ${Number(result?.coverage?.total_count || 0)} 条`);
            await refreshAll(true);
        } catch (err) {
            if (el("news-action-output")) el("news-action-output").textContent = `历史原文补拉失败: ${err.message}`;
            notify(`历史原文补拉失败: ${err.message}`, true);
        } finally {
            state.pulling = false;
        }
    }

    async function backfillNow() {
        if (state.pulling) return;
        state.pulling = true;
        try {
            const result = await request("/worker/backfill_recent?background=true", {
                method: "POST",
                body: JSON.stringify({
                    hours: Math.max(24, Math.min(720, Number(getVal("news-hours", "24")) || 24)),
                    max_candidates: 180,
                    force_reprocess_done: false,
                }),
                timeoutMs: 20000,
            });
            if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(result, null, 2);
            let finalResult = result;
            if (result?.queued && result?.job_id) {
                notify(`历史回补任务已排队: ${result.job_id}`);
                finalResult = (await waitLlmJob(result.job_id))?.result || result;
                if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(finalResult, null, 2);
            }
            const candidateCount = Number(finalResult?.candidate_count || finalResult?.backfill?.candidate_count || 0);
            const eventsCount = Number(finalResult?.events_count || finalResult?.backfill?.events_count || 0);
            notify(`历史回补完成: 候选 ${candidateCount}，新增结构化 ${eventsCount}`);
            await refreshAll(true);
        } catch (err) {
            if (el("news-action-output")) el("news-action-output").textContent = `历史回补失败: ${err.message}`;
            notify(`历史回补失败: ${err.message}`, true);
        } finally {
            state.pulling = false;
        }
    }

    async function requeueFailedTasks() {
        if (state.pulling) return;
        state.pulling = true;
        try {
            const result = await request("/worker/requeue", {
                method: "POST",
                body: JSON.stringify({ statuses: ["failed"], limit: 200 }),
                timeoutMs: 20000,
            });
            if (el("news-action-output")) el("news-action-output").textContent = JSON.stringify(result, null, 2);
            const n = Number(result?.requeue?.requeued_count || 0);
            notify(`已启动失败历史补跑: ${n} 条`);
            await refreshAll(true);
        } catch (err) {
            if (el("news-action-output")) el("news-action-output").textContent = `失败历史补跑失败: ${err.message}`;
            notify(`失败历史补跑失败: ${err.message}`, true);
        } finally {
            state.pulling = false;
        }
    }

    function restartTimer() {
        if (state.timer) clearInterval(state.timer);
        state.timer = setInterval(() => {
            if (document.hidden || !isNewsVisible()) return;
            refreshAll(false).catch(() => {});
        }, Math.max(5, Number(getVal("news-auto-refresh-sec", "15")) || 15) * 1000);
    }

    function connectWs() {
        try {
            const protocol = location.protocol === "https:" ? "wss" : "ws";
            const ws = new WebSocket(`${protocol}://${location.host}/ws`);
            ws.onmessage = (evt) => {
                try {
                    const msg = JSON.parse(evt.data || "{}");
                    if (msg?.event !== "news_update") return;
                    if (isNewsVisible()) refreshAll(false).catch(() => {});
                    else state.needsRefresh = true;
                } catch (_) {}
            };
            ws.onclose = () => setTimeout(connectWs, 2000);
        } catch (_) {}
    }

    function bind() {
        el("news-refresh-btn")?.addEventListener("click", () => refreshAll(true));
        el("news-pull-btn")?.addEventListener("click", pullNow);
        el("news-archive-backfill-btn")?.addEventListener("click", backfillArchiveNow);
        el("news-backfill-btn")?.addEventListener("click", backfillNow);
        el("news-requeue-btn")?.addEventListener("click", requeueFailedTasks);
        ["news-hours", "news-symbol", "news-max-records"].forEach((id) => el(id)?.addEventListener("change", () => refreshAll(true)));
        el("news-auto-refresh-sec")?.addEventListener("change", restartTimer);
        el("news-bucket-granularity")?.addEventListener("change", () => {
            if (!state.summary && !state.summaryLoading && isNewsVisible()) {
                state.summaryLoading = true;
                loadSummary().then((summary) => {
                    state.summary = summary || null;
                    state.summaryLoadedAt = Date.now();
                    renderAll();
                }).catch(() => {}).finally(() => {
                    state.summaryLoading = false;
                });
                return;
            }
            renderBuckets();
        });
        el("news-summary-granularity")?.addEventListener("change", renderSummary);
        document.querySelectorAll('.tab-btn[data-tab="news"]').forEach((btn) => btn.addEventListener("click", () => {
            setTimeout(() => {
                restartTimer();
                if (state.needsRefresh || !state.latest) refreshAll(true).catch(() => {});
                else scheduleBucketRender(220);
            }, 120);
        }));
        document.addEventListener("visibilitychange", () => {
            if (!document.hidden && isNewsVisible()) {
                refreshAll(false).catch(() => {});
                scheduleBucketRender(200);
            }
        });
        window.addEventListener("resize", () => scheduleBucketRender(180));
    }

    async function init() {
        if (!el("news-unstructured-list")) return;
        bind();
        restartTimer();
        connectWs();
        if (isNewsVisible()) {
            await refreshAll(true);
            scheduleBucketRender(220);
        }
        else state.needsRefresh = true;
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
    else init();
})();


