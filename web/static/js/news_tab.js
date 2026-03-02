(function () {
    const API_BASE = "/api/news";
    const state = { timer: null, ws: null, summary: null, latest: null, health: null, pulling: false };

    function esc(v) {
        return String(v ?? "").replace(/[&<>"']/g, (m) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
        }[m]));
    }

    function notify(msg, isError = false) {
        if (typeof window.notify === "function") {
            window.notify(msg, isError);
            return;
        }
        const box = document.getElementById("notification");
        if (!box) return;
        box.textContent = msg;
        box.className = `notification show ${isError ? "error" : ""}`;
        setTimeout(() => box.classList.remove("show"), 2600);
    }

    function parseTs(value) {
        if (!value) return null;
        let text = String(value).trim();
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(text)) text += "Z";
        const d = new Date(text);
        return Number.isFinite(d.getTime()) ? d : null;
    }

    function fmtTs(value) {
        const d = parseTs(value);
        return d ? d.toLocaleString("zh-CN", { hour12: false }) : "--";
    }

    function fmtLatency(sec) {
        const value = Number(sec || 0);
        if (!Number.isFinite(value) || value <= 0) return "--";
        if (value < 60) return `${value.toFixed(1)}s`;
        return `${(value / 60).toFixed(1)}m`;
    }

    function sentimentClass(v) {
        if (Number(v) > 0) return "news-sentiment-pos";
        if (Number(v) < 0) return "news-sentiment-neg";
        return "news-sentiment-neu";
    }

    function sentimentText(v) {
        if (Number(v) > 0) return "正面";
        if (Number(v) < 0) return "负面";
        return "中性";
    }

    function summarySentimentClass(v) {
        if (v === "positive") return "news-sentiment-pos";
        if (v === "negative") return "news-sentiment-neg";
        return "news-sentiment-neu";
    }

    function summarySentimentText(v) {
        if (v === "positive") return "利好";
        if (v === "negative") return "利空";
        return "中性";
    }

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timeoutMs = Math.max(3000, Number(options.timeoutMs || 20000));
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const sep = path.includes("?") ? "&" : "?";
        try {
            const response = await fetch(`${API_BASE}${path}${sep}_ts=${Date.now()}`, {
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
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(payload.detail || payload.error || `请求失败(${response.status})`);
            return payload;
        } catch (e) {
            if (e?.name === "AbortError") throw new Error(`请求超时(${timeoutMs}ms): ${path}`);
            throw e;
        } finally {
            clearTimeout(timer);
        }
    }

    const getSymbol = () => String(document.getElementById("news-symbol")?.value || "").trim().toUpperCase();
    const getHours = () => Math.max(1, Number(document.getElementById("news-hours")?.value || 24));
    const getAutoRefreshSec = () => Math.max(5, Number(document.getElementById("news-auto-refresh-sec")?.value || 15));
    const getMaxRecords = () => Math.max(20, Math.min(250, Number(document.getElementById("news-max-records")?.value || 120)));
    const getGranularity = () => String(document.getElementById("news-bucket-granularity")?.value || "1h");
    const getSummaryGranularity = () => String(document.getElementById("news-summary-granularity")?.value || "1h");

    function computeFeedStats(items) {
        const rows = Array.isArray(items) ? items : [];
        const out = { total: rows.length, structured: 0, unstructured: 0, sentiment: { positive: 0, neutral: 0, negative: 0 } };
        for (const item of rows) {
            if (item?.has_event) {
                out.structured += 1;
                const s = Number(item.sentiment || 0);
                if (s > 0) out.sentiment.positive += 1;
                else if (s < 0) out.sentiment.negative += 1;
                else out.sentiment.neutral += 1;
            } else {
                out.unstructured += 1;
                const s = String(item.summary_sentiment || "neutral").toLowerCase();
                if (s === "positive") out.sentiment.positive += 1;
                else if (s === "negative") out.sentiment.negative += 1;
                else out.sentiment.neutral += 1;
            }
        }
        return out;
    }

    function latestBucketStats(summary, granularity) {
        const buckets = Array.isArray(summary?.bucket_stats?.[granularity]) ? summary.bucket_stats[granularity] : [];
        const row = buckets.length ? buckets[buckets.length - 1] : null;
        if (!row) {
            return {
                total: 0,
                sentiment: { positive: 0, neutral: 0, negative: 0 },
                bucket_count: buckets.length,
                bucket_start: null,
            };
        }
        return {
            total: Number(row?.count || 0),
            sentiment: {
                positive: Number(row?.positive || 0),
                neutral: Number(row?.neutral || 0),
                negative: Number(row?.negative || 0),
            },
            bucket_count: buckets.length,
            bucket_start: row?.bucket_start || null,
        };
    }

    function renderProviderStats(summary, latestSourceStats) {
        const box = document.getElementById("news-source-stats");
        if (!box) return;
        const sourceSummary = summary?.source_summary || {};
        const byProviderSummary = summary?.by_provider || {};
        const currentBySource = latestSourceStats?.by_source || latestSourceStats?.by_provider || {};
        const names = Array.from(new Set([...Object.keys(sourceSummary), ...Object.keys(byProviderSummary), ...Object.keys(currentBySource)]))
            .filter((name) => name && name !== "event" && name !== "legacy");
        if (!names.length) {
            box.innerHTML = '<div class="list-item">暂无来源统计</div>';
            return;
        }
        names.sort((a, b) => {
            const av = Number(sourceSummary[a]?.inserted_count || byProviderSummary[a] || currentBySource[a] || 0);
            const bv = Number(sourceSummary[b]?.inserted_count || byProviderSummary[b] || currentBySource[b] || 0);
            return bv - av;
        });
        box.innerHTML = names.slice(0, 12).map((name) => {
            const total24h = Number(sourceSummary[name]?.inserted_count || byProviderSummary[name] || 0);
            const latestFeed = Number(currentBySource[name] || 0);
            const p95 = fmtLatency(sourceSummary[name]?.latency_p95);
            const fail = Number(sourceSummary[name]?.failure_rate || 0);
            return `<div class="list-item"><span>${esc(name)}</span><span>24h原始 ${total24h} | 当前流 ${latestFeed} | P95 ${p95} | 失败率 ${(fail * 100).toFixed(0)}%</span></div>`;
        }).join("");
    }

    function renderHealth(summary, health) {
        const badge = document.getElementById("news-health-badge");
        const clock = document.getElementById("news-now-time");
        const queue = summary?.llm_queue || health?.llm_queue || {};
        const sourceStates = Array.isArray(summary?.source_states) ? summary.source_states : (Array.isArray(health?.source_states) ? health.source_states : []);
        const errorSources = sourceStates.filter((x) => Number(x?.error_count || 0) > 0).length;
        if (badge) {
            badge.className = `status-badge ${errorSources > 0 ? "warning" : "connected"}`;
            badge.textContent = `源 ${sourceStates.length} | 待抽取 ${Number(queue?.pending_total || 0)} | 异常 ${errorSources}`;
        }
        if (clock) {
            clock.textContent = fmtTs((health || {}).timestamp || new Date().toISOString());
        }
    }

    function updateSummary(summary, latest) {
        const feedStats = latest?.feed_stats || computeFeedStats(latest?.items || []);
        const granularity = getSummaryGranularity();
        const useFeed = granularity === "feed";
        const shown = useFeed ? feedStats : latestBucketStats(summary, granularity);

        const setMetric = (valueId, value, labelText) => {
            const valueEl = document.getElementById(valueId);
            if (!valueEl) return;
            valueEl.textContent = String(Number(value || 0));
            const labelEl = valueEl.parentElement?.querySelector?.(".label");
            if (labelEl && labelText) labelEl.textContent = labelText;
        };

        if (useFeed) {
            setMetric("news-events-count", shown?.total, "当前列表总条数");
            setMetric("news-positive-count", shown?.sentiment?.positive, "利好（当前列表）");
            setMetric("news-neutral-count", shown?.sentiment?.neutral, "中性（当前列表）");
            setMetric("news-negative-count", shown?.sentiment?.negative, "利空（当前列表）");
        } else {
            setMetric("news-events-count", shown?.total, `总事件数（${granularity} 最新桶）`);
            setMetric("news-positive-count", shown?.sentiment?.positive, `正面事件数（${granularity}）`);
            setMetric("news-neutral-count", shown?.sentiment?.neutral, `中性事件数（${granularity}）`);
            setMetric("news-negative-count", shown?.sentiment?.negative, `负面事件数（${granularity}）`);
        }

        renderProviderStats(summary, latest?.source_stats || {});

        const meta = document.getElementById("news-summary-gran-meta");
        if (meta) {
            meta.textContent = useFeed
                ? `当前列表口径 | ${Number(feedStats?.total || 0)} 条`
                : `${granularity} 最新桶 | ${shown?.bucket_start ? fmtTs(shown.bucket_start) : "--"}`;
        }

        const note = document.getElementById("news-summary-note");
        if (note) {
            const structured24 = Number(summary?.events_count || 0);
            const structuredInFeed = Number(feedStats?.structured || 0);
            const unstructuredInFeed = Number(feedStats?.unstructured || 0);
            const modeText = useFeed ? "当前列表（与下方新闻流一致）" : `结构化事件最新桶统计（${granularity}）`;
            note.innerHTML = [
                `<div class="list-item"><span>统计口径</span><span>${modeText}</span></div>`,
                `<div class="list-item"><span>24h结构化事件库</span><span>${structured24} 条</span></div>`,
                `<div class="list-item"><span>当前列表结构化 / 未结构化</span><span>${structuredInFeed} / ${unstructuredInFeed}</span></div>`,
                `<div class="list-item"><span>当前列表条数</span><span>${Number(latest?.count || 0)}（回看 ${Number(summary?.hours || 24)}h）</span></div>`,
                `<div class="list-item"><span>LLM待抽取队列</span><span>${Number(summary?.llm_queue?.pending_total || 0)} 条</span></div>`,
            ].join("");
        }
    }

    function renderBucketStats(summary) {
        const chartEl = document.getElementById("news-bucket-chart");
        const listEl = document.getElementById("news-bucket-list");
        const metaEl = document.getElementById("news-bucket-meta");
        const gran = getGranularity();
        const buckets = summary?.bucket_stats?.[gran] || [];
        if (metaEl) metaEl.textContent = `结构化事件数（${gran}） | 桶数 ${buckets.length} | 24h事件 ${Number(summary?.events_count || 0)}`;
        if (!buckets.length) {
            if (chartEl) chartEl.innerHTML = '<div class="list-item">暂无结构化事件统计数据</div>';
            if (listEl) listEl.innerHTML = '<div class="list-item">暂无结构化事件统计数据</div>';
            return;
        }
        const recent = buckets.slice(-36);
        if (chartEl) {
            chartEl.style.display = "block";
            chartEl.style.minHeight = "320px";
        }
        if (chartEl && typeof Plotly !== "undefined") {
            const draw = () => {
                if (!chartEl || chartEl.offsetWidth < 40 || chartEl.offsetHeight < 40) {
                    setTimeout(() => renderBucketStats(summary), 180);
                    return;
                }
                try {
                    if (typeof Plotly.purge === "function") Plotly.purge(chartEl);
                    chartEl.innerHTML = "";
                    const x = recent.map((row) => parseTs(row.bucket_start) || row.bucket_start);
                    const total = recent.map((row) => Number(row.count || 0));
                    const pos = recent.map((row) => Number(row.positive || 0));
                    const neg = recent.map((row) => Number(row.negative || 0));
                    Plotly.react(chartEl, [
                        { type: "bar", x, y: total, name: "总事件数", marker: { color: "#1f9d63", opacity: 0.45 } },
                        { type: "scatter", mode: "lines+markers", x, y: pos, name: "正面", line: { color: "#20bf78", width: 2 } },
                        { type: "scatter", mode: "lines+markers", x, y: neg, name: "负面", line: { color: "#ea5b61", width: 2 } },
                    ], {
                        paper_bgcolor: "#111723",
                        plot_bgcolor: "#111723",
                        font: { color: "#d7dde8" },
                        margin: { l: 48, r: 32, t: 16, b: 40 },
                        xaxis: { showgrid: true, gridcolor: "#283242", automargin: true },
                        yaxis: { showgrid: true, gridcolor: "#283242", rangemode: "tozero", automargin: true },
                        legend: { orientation: "h", y: 1.12 },
                        barmode: "overlay",
                        hovermode: "x unified",
                    }, { responsive: true, displaylogo: false });
                    setTimeout(() => { try { Plotly.Plots.resize(chartEl); } catch (_) {} }, 80);
                    setTimeout(() => { try { Plotly.Plots.resize(chartEl); } catch (_) {} }, 300);
                } catch (e) {
                    chartEl.innerHTML = `<div class="list-item">图表渲染失败：${esc(e?.message || e)}</div>`;
                }
            };
            if (document.hidden) setTimeout(draw, 180);
            else requestAnimationFrame(draw);
        } else if (chartEl) {
            chartEl.innerHTML = '<div class="list-item">图表库未加载，请查看下方列表</div>';
        }
        if (listEl) {
            listEl.innerHTML = recent.slice().reverse().map((row) =>
                `<div class="list-item"><span>${fmtTs(row.bucket_start)}</span><span>总${Number(row.count || 0)} | +${Number(row.positive || 0)} / 0:${Number(row.neutral || 0)} / -${Number(row.negative || 0)}</span></div>`
            ).join("");
        }
    }

    function renderUnstructured(items) {
        const box = document.getElementById("news-unstructured-list");
        const counter = document.getElementById("news-unstructured-count");
        if (!box) return;
        const rows = (items || []).filter((x) => !x.has_event);
        if (counter) counter.textContent = `${rows.length} 条`;
        if (!rows.length) {
            box.innerHTML = '<div class="list-item news-white">暂无未结构化新闻</div>';
            return;
        }
        box.innerHTML = rows.map((item) => {
            const title = esc(item.summary_title || item.title || "（无标题）");
            const url = String(item.url || "").trim();
            const source = esc(item.source || "-");
            const provider = esc(item.provider || "-");
            const tsText = fmtTs(item.published_at);
            const ss = item.summary_sentiment || "neutral";
            const titleHtml = url ? `<a class="news-title news-white" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${title}</a>` : `<span class="news-title news-white">${title}</span>`;
            return `<div class="list-item news-row"><div class="news-main">${titleHtml}<div class="news-meta"><span>${tsText}</span><span class="news-tag-white">${provider}</span><span class="${summarySentimentClass(ss)}">${summarySentimentText(ss)}</span><span>${source}</span></div></div></div>`;
        }).join("");
    }

    function renderStructured(items) {
        const box = document.getElementById("news-structured-list");
        const counter = document.getElementById("news-structured-count");
        if (!box) return;
        const rows = (items || []).filter((x) => !!x.has_event);
        if (counter) counter.textContent = `${rows.length} 条`;
        if (!rows.length) {
            box.innerHTML = '<div class="list-item news-white">暂无已结构化事件</div>';
            return;
        }
        box.innerHTML = rows.map((item) => {
            const title = esc(item.summary_title || item.title || "（无标题）");
            const url = String(item.url || "").trim();
            const source = esc(item.source || "-");
            const provider = esc(item.provider || "-");
            const symbol = esc(item.symbol || "-");
            const eventType = esc(item.event_type || "raw");
            const impact = Number(item.impact_score || 0);
            const s = Number(item.sentiment || 0);
            const tsText = fmtTs(item.published_at);
            const titleHtml = url ? `<a class="news-title news-white" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${title}</a>` : `<span class="news-title news-white">${title}</span>`;
            return `<div class="list-item news-row"><div class="news-main">${titleHtml}<div class="news-meta"><span>${tsText}</span><span class="news-tag-white">${provider}</span><span class="news-tag-white">${symbol}</span><span class="news-tag-white">${eventType}</span><span class="${sentimentClass(s)}">${sentimentText(s)}</span><span>impact ${impact.toFixed(3)}</span><span>${source}</span></div></div></div>`;
        }).join("");
    }

    async function loadSummary() {
        const params = new URLSearchParams({ hours: String(getHours()), feed_limit: String(getMaxRecords()) });
        const symbol = getSymbol();
        if (symbol) params.set("symbol", symbol);
        return request(`/summary?${params.toString()}`, { timeoutMs: 25000 });
    }

    async function loadHealth() {
        return request("/health", { timeoutMs: 12000 });
    }

    async function loadFeed(useSummarize = false) {
        const symbol = getSymbol();
        const hours = getHours();
        const maxRecords = getMaxRecords();
        const params = new URLSearchParams({
            hours: String(hours),
            limit: String(useSummarize ? Math.min(60, maxRecords) : maxRecords),
            summarize: useSummarize ? "true" : "false",
        });
        if (symbol) params.set("symbol", symbol);
        return request(`/latest?${params.toString()}`, { timeoutMs: useSummarize ? 65000 : 25000 });
    }

    function applyData(summary, latest) {
        state.summary = summary || null;
        state.latest = latest || null;
        updateSummary(summary, latest);
        renderBucketStats(summary || {});
        renderUnstructured(latest?.items || []);
        renderStructured(latest?.items || []);
        renderHealth(summary || {}, state.health || {});
    }

    async function refreshAll() {
        try {
            const [summary, latest, health] = await Promise.all([loadSummary(), loadFeed(false), loadHealth()]);
            state.health = health || null;
            applyData(summary, latest);
        } catch (e) {
            try {
                const [summary, latest, health] = await Promise.all([loadSummary(), loadFeed(false), loadHealth()]);
                state.health = health || null;
                applyData(summary, latest);
                notify("新闻接口较慢，已回退到快速刷新");
            } catch (e2) {
                notify(`新闻刷新失败: ${e2.message || e.message}`, true);
            }
        }
    }

    async function waitPullJob(jobId, timeoutMs = 12 * 60 * 1000) {
        const started = Date.now();
        while (Date.now() - started < timeoutMs) {
            const status = await request("/pull_status", { timeoutMs: 15000 });
            const jobs = Array.isArray(status?.jobs) ? status.jobs : [];
            const job = jobs.find((x) => String(x?.job_id || "") === String(jobId || ""));
            if (job?.status === "completed") return job;
            if (job?.status === "failed") throw new Error(job?.error || "后台结构化失败");
            await new Promise((resolve) => setTimeout(resolve, 2500));
        }
        throw new Error(`后台新闻任务超时: ${jobId}`);
    }

    async function pullNow() {
        if (state.pulling) return;
        state.pulling = true;
        const output = document.getElementById("news-action-output");
        try {
            const data = await request("/pull_now", {
                method: "POST",
                timeoutMs: 20000,
                body: JSON.stringify({ since_minutes: Math.max(30, Math.min(1440, getHours() * 60)), max_records: getMaxRecords() }),
            });
            if (output) output.textContent = JSON.stringify(data, null, 2);
            if (data?.queued && data?.job_id) {
                notify(`后台新闻任务已启动: ${data.job_id}`);
                const job = await waitPullJob(data.job_id);
                if (output) output.textContent = JSON.stringify(job?.result || job, null, 2);
                notify(`后台结构化完成：新增事件 ${Number(job?.result?.events_count || 0)} 条`);
            } else {
                notify(`拉取完成：新增事件 ${Number(data?.events_count || 0)} 条`);
            }
            await refreshAll();
        } catch (e) {
            if (output) output.textContent = `拉取失败: ${e.message}`;
            notify(`拉取失败: ${e.message}`, true);
        } finally {
            state.pulling = false;
        }
    }

    function restartTimer() {
        if (state.timer) clearInterval(state.timer);
        state.timer = setInterval(refreshAll, getAutoRefreshSec() * 1000);
    }

    function connectWs() {
        try {
            const proto = location.protocol === "https:" ? "wss" : "ws";
            const ws = new WebSocket(`${proto}://${location.host}/ws`);
            state.ws = ws;
            ws.onmessage = (evt) => {
                try {
                    const msg = JSON.parse(evt.data || "{}");
                    if (msg?.event === "news_update") refreshAll();
                } catch (_) {}
            };
            ws.onclose = () => setTimeout(connectWs, 2000);
        } catch (_) {}
    }

    function bindVisibilityRefresh() {
        document.querySelectorAll('.tab-btn[data-tab="news"]').forEach((btn) => {
            btn.addEventListener("click", () => {
                setTimeout(() => {
                    renderBucketStats(state.summary || {});
                    refreshAll().catch(() => {});
                }, 80);
            });
        });
        document.addEventListener("visibilitychange", () => {
            if (!document.hidden) refreshAll().catch(() => {});
        });
    }

    function bindActions() {
        document.getElementById("news-refresh-btn")?.addEventListener("click", refreshAll);
        document.getElementById("news-pull-btn")?.addEventListener("click", pullNow);
        document.getElementById("news-auto-refresh-sec")?.addEventListener("change", restartTimer);
        document.getElementById("news-hours")?.addEventListener("change", refreshAll);
        document.getElementById("news-symbol")?.addEventListener("change", refreshAll);
        document.getElementById("news-max-records")?.addEventListener("change", refreshAll);
        document.getElementById("news-bucket-granularity")?.addEventListener("change", () => renderBucketStats(state.summary || {}));
        document.getElementById("news-summary-granularity")?.addEventListener("change", () => updateSummary(state.summary || {}, state.latest || {}));
        bindVisibilityRefresh();
    }

    async function init() {
        if (!document.getElementById("news-unstructured-list")) return;
        bindActions();
        restartTimer();
        connectWs();
        await refreshAll();
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
    else init();
})();
