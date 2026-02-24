(function () {
    const API_BASE = "/api/news";
    const state = {
        timer: null,
        ws: null,
        pulling: false,
        latest: null,
        summary: null,
    };

    function esc(value) {
        return String(value ?? "").replace(/[&<>"']/g, (m) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            "\"": "&quot;",
            "'": "&#39;",
        }[m]));
    }

    function notify(msg, isError = false) {
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

    function sentimentClass(sentiment) {
        if (Number(sentiment) > 0) return "news-sentiment-pos";
        if (Number(sentiment) < 0) return "news-sentiment-neg";
        return "news-sentiment-neu";
    }

    function sentimentText(sentiment) {
        if (Number(sentiment) > 0) return "正面";
        if (Number(sentiment) < 0) return "负面";
        return "中性";
    }

    function summarySentimentClass(sentiment) {
        if (sentiment === "positive") return "news-sentiment-pos";
        if (sentiment === "negative") return "news-sentiment-neg";
        return "news-sentiment-neu";
    }

    function summarySentimentText(sentiment) {
        if (sentiment === "positive") return "利好";
        if (sentiment === "negative") return "利空";
        return "中性";
    }

    function setNowTime() {
        const el = document.getElementById("news-now-time");
        if (!el) return;
        el.textContent = new Date().toLocaleString("zh-CN", { hour12: false });
    }

    function setHealth(ok, text) {
        const badge = document.getElementById("news-health-badge");
        if (!badge) return;
        badge.textContent = text || (ok ? "运行中" : "异常");
        badge.style.background = ok
            ? "linear-gradient(135deg, #137f49, #20bf78)"
            : "linear-gradient(135deg, #c9484f, #ea5b61)";
    }

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timeoutMs = Math.max(3000, Number(options.timeoutMs || 22000));
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const sep = path.includes("?") ? "&" : "?";
        const finalPath = `${path}${sep}_ts=${Date.now()}`;
        try {
            const response = await fetch(`${API_BASE}${finalPath}`, {
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
            if (!response.ok) {
                throw new Error(payload.detail || payload.error || `请求失败(${response.status})`);
            }
            return payload;
        } catch (err) {
            if (err?.name === "AbortError") {
                throw new Error(`请求超时(${timeoutMs}ms): ${path}`);
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    function getSymbolInput() {
        return String(document.getElementById("news-symbol")?.value || "").trim().toUpperCase();
    }
    function getHoursInput() {
        return Math.max(1, Number(document.getElementById("news-hours")?.value || 24));
    }
    function getSummaryGranularity() {
        return String(document.getElementById("news-summary-granularity")?.value || "1h");
    }
    function getAutoRefreshSec() {
        return Math.max(5, Number(document.getElementById("news-auto-refresh-sec")?.value || 15));
    }
    function getMaxRecordsInput() {
        return Math.max(20, Math.min(250, Number(document.getElementById("news-max-records")?.value || 120)));
    }

    function computeFeedStats(items) {
        const rows = Array.isArray(items) ? items : [];
        const stats = { total: rows.length, structured: 0, unstructured: 0, sentiment: { positive: 0, neutral: 0, negative: 0 } };
        for (const item of rows) {
            if (item?.has_event) {
                stats.structured += 1;
                const s = Number(item.sentiment || 0);
                if (s > 0) stats.sentiment.positive += 1;
                else if (s < 0) stats.sentiment.negative += 1;
                else stats.sentiment.neutral += 1;
            } else {
                stats.unstructured += 1;
                const ss = String(item?.summary_sentiment || "neutral").toLowerCase();
                if (ss === "positive") stats.sentiment.positive += 1;
                else if (ss === "negative") stats.sentiment.negative += 1;
                else stats.sentiment.neutral += 1;
            }
        }
        return stats;
    }

    function latestBucketStats(summary, granularity) {
        const buckets = Array.isArray(summary?.bucket_stats?.[granularity]) ? summary.bucket_stats[granularity] : [];
        const row = buckets.length ? buckets[buckets.length - 1] : null;
        if (!row) {
            return {
                total: 0,
                structured: 0,
                unstructured: 0,
                sentiment: { positive: 0, neutral: 0, negative: 0 },
                bucket_count: buckets.length,
                bucket_start: null,
            };
        }
        return {
            total: Number(row?.count || 0),
            structured: Number(row?.count || 0),
            unstructured: 0,
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
        const byProviderSummary = summary?.by_provider || {};
        const byProviderLatest = latestSourceStats?.by_provider || {};
        const names = Array.from(new Set([...Object.keys(byProviderSummary), ...Object.keys(byProviderLatest)]));
        if (!names.length) {
            box.innerHTML = '<div class="list-item">暂无来源统计</div>';
            return;
        }
        names.sort((a, b) => Number(byProviderSummary[b] || byProviderLatest[b] || 0) - Number(byProviderSummary[a] || byProviderLatest[a] || 0));
        box.innerHTML = names.slice(0, 12).map((name) => {
            const total24h = Number(byProviderSummary[name] || 0);
            const latestFeed = Number(byProviderLatest[name] || 0);
            return `<div class="list-item"><span>${esc(name)}</span><span>24h原始 ${total24h} | 当前流 ${latestFeed}</span></div>`;
        }).join("");
    }

    function updateSummary(summary, latest) {
        const feedStats = latest?.feed_stats || computeFeedStats(latest?.items || []);
        const summaryGranularity = (typeof getSummaryGranularity === "function") ? getSummaryGranularity() : "1h";
        const useFeed = summaryGranularity === "feed";
        const shownStats = useFeed ? feedStats : latestBucketStats(summary, summaryGranularity);

        const setMetric = (valueId, value, labelText) => {
            const valueEl = document.getElementById(valueId);
            if (valueEl) {
                valueEl.textContent = String(Number(value || 0));
                const labelEl = valueEl.parentElement?.querySelector?.('.label');
                if (labelEl && labelText) labelEl.textContent = labelText;
            }
        };

        if (useFeed) {
            setMetric("news-events-count", shownStats?.total, "\u5f53\u524d\u5217\u8868\u603b\u6761\u6570");
            setMetric("news-positive-count", shownStats?.sentiment?.positive, "\u5229\u597d\uff08\u5f53\u524d\u5217\u8868\uff09");
            setMetric("news-neutral-count", shownStats?.sentiment?.neutral, "\u4e2d\u6027\uff08\u5f53\u524d\u5217\u8868\uff09");
            setMetric("news-negative-count", shownStats?.sentiment?.negative, "\u5229\u7a7a\uff08\u5f53\u524d\u5217\u8868\uff09");
        } else {
            setMetric("news-events-count", shownStats?.total, `\u603b\u4e8b\u4ef6\u6570\uff08${summaryGranularity}\u6700\u65b0\u6876\uff09`);
            setMetric("news-positive-count", shownStats?.sentiment?.positive, `\u6b63\u9762\u4e8b\u4ef6\u6570\uff08${summaryGranularity}\uff09`);
            setMetric("news-neutral-count", shownStats?.sentiment?.neutral, `\u4e2d\u6027\u4e8b\u4ef6\u6570\uff08${summaryGranularity}\uff09`);
            setMetric("news-negative-count", shownStats?.sentiment?.negative, `\u8d1f\u9762\u4e8b\u4ef6\u6570\uff08${summaryGranularity}\uff09`);
        }

        renderProviderStats(summary, latest?.source_stats || {});

        const granMeta = document.getElementById("news-summary-gran-meta");
        if (granMeta) {
            if (useFeed) {
                granMeta.textContent = `\u5f53\u524d\u5217\u8868\u540c\u53e3\u5f84 | ${Number(feedStats?.total || 0)} \u6761`;
            } else {
                const bucketStart = shownStats?.bucket_start ? fmtTs(shownStats.bucket_start) : "--";
                granMeta.textContent = `${summaryGranularity} \u6700\u65b0\u6876 | ${bucketStart}`;
            }
        }

        const note = document.getElementById("news-summary-note");
        if (note) {
            const structured24 = Number(summary?.events_count || 0);
            const structuredInFeed = Number(feedStats?.structured || 0);
            const unstructuredInFeed = Number(feedStats?.unstructured || 0);
            const modeText = useFeed
                ? "\u5f53\u524d\u5217\u8868\uff08\u4e0e\u4e0b\u65b9\u65b0\u95fb\u6d41\u4e00\u81f4\uff09"
                : `\u7ed3\u6784\u5316\u4e8b\u4ef6\u6700\u65b0\u6876\u7edf\u8ba1\uff08${summaryGranularity}\uff09`;
            note.innerHTML = [
                `<div class="list-item"><span>\u7edf\u8ba1\u53e3\u5f84</span><span>${modeText}</span></div>`,
                `<div class="list-item"><span>\u8bf4\u660e</span><span>\u9876\u90e8\u56db\u9879\u4e0e\u4e0b\u65b9\u6765\u6e90\u7edf\u8ba1\u4e0d\u540c\u53e3\u5f84\uff08\u6765\u6e90\u7edf\u8ba1\u56fa\u5b9a\u663e\u793a24h\u539f\u59cb/\u5f53\u524d\u6d41\uff09</span></div>`,
                `<div class="list-item"><span>24h\u7ed3\u6784\u5316\u4e8b\u4ef6\u5e93</span><span>${structured24} \u6761</span></div>`,
                `<div class="list-item"><span>\u5f53\u524d\u5217\u8868\u7ed3\u6784\u5316/\u672a\u7ed3\u6784\u5316</span><span>${structuredInFeed} / ${unstructuredInFeed}</span></div>`,
                `<div class="list-item"><span>\u5f53\u524d\u5217\u8868\u6761\u6570</span><span>${Number(latest?.count || 0)}\uff08\u56de\u770b ${Number(summary?.hours || 24)}h\uff09</span></div>`,
            ].join("");
        }
    }

    function renderBucketStats(summary) {
        const chartEl = document.getElementById("news-bucket-chart");
        const listEl = document.getElementById("news-bucket-list");
        const metaEl = document.getElementById("news-bucket-meta");
        const gran = String(document.getElementById("news-bucket-granularity")?.value || "1h");
        const buckets = summary?.bucket_stats?.[gran] || [];

        if (metaEl) {
            metaEl.textContent = `结构化事件数（${gran}）| 桶数 ${buckets.length} | 24h事件 ${Number(summary?.events_count || 0)}`;
        }

        if (!buckets.length) {
            if (chartEl) chartEl.innerHTML = '<div class="list-item">暂无结构化事件统计数据</div>';
            if (listEl) listEl.innerHTML = '<div class="list-item">暂无结构化事件统计数据</div>';
            return;
        }

        const recent = buckets.slice(-36);
        if (chartEl && typeof Plotly !== "undefined") {
            chartEl.innerHTML = "";
            const x = recent.map((x) => parseTs(x.bucket_start) || x.bucket_start);
            const total = recent.map((x) => Number(x.count || 0));
            const pos = recent.map((x) => Number(x.positive || 0));
            const neg = recent.map((x) => Number(x.negative || 0));
            try {
                if (typeof Plotly.purge === "function") Plotly.purge(chartEl);
                Plotly.react(
                    chartEl,
                    [
                        { type: "bar", x, y: total, name: "总事件数", marker: { color: "#1f9d63", opacity: 0.45 } },
                        { type: "scatter", mode: "lines+markers", x, y: pos, name: "正面", line: { color: "#20bf78", width: 2 } },
                        { type: "scatter", mode: "lines+markers", x, y: neg, name: "负面", line: { color: "#ea5b61", width: 2 } },
                    ],
                    {
                        paper_bgcolor: "#111723",
                        plot_bgcolor: "#111723",
                        font: { color: "#d7dde8" },
                        margin: { l: 40, r: 24, t: 16, b: 36 },
                        xaxis: { showgrid: true, gridcolor: "#283242" },
                        yaxis: { showgrid: true, gridcolor: "#283242", rangemode: "tozero" },
                        legend: { orientation: "h", y: 1.12 },
                        barmode: "overlay",
                        hovermode: "x unified",
                    },
                    { responsive: true, displaylogo: false }
                );
                setTimeout(() => { try { Plotly.Plots.resize(chartEl); } catch (_) {} }, 50);
                setTimeout(() => { try { Plotly.Plots.resize(chartEl); } catch (_) {} }, 300);
            } catch (e) {
                chartEl.innerHTML = `<div class="list-item">图表渲染失败: ${esc(e?.message || e)}</div>`;
            }
        } else if (chartEl) {
            chartEl.innerHTML = '<div class="list-item">图表库未加载，改看下方统计列表</div>';
        }

        if (listEl) {
            listEl.innerHTML = "";
            listEl.innerHTML = recent.slice().reverse().map((row) => {
                const ts = fmtTs(row.bucket_start);
                return `<div class="list-item"><span>${ts}</span><span>总${Number(row.count || 0)} | +${Number(row.positive || 0)} / 0:${Number(row.neutral || 0)} / -${Number(row.negative || 0)}</span></div>`;
            }).join("");
        }
    }

    function renderUnstructuredNews(items) {
        const box = document.getElementById("news-unstructured-list");
        const counter = document.getElementById("news-unstructured-count");
        if (!box) return;
        const unstructured = (items || []).filter((item) => !item.has_event);
        if (counter) counter.textContent = `${unstructured.length} 条`;
        if (!unstructured.length) {
            box.innerHTML = '<div class="list-item news-white">暂无未结构化新闻</div>';
            return;
        }
        box.innerHTML = unstructured.map((item) => {
            const title = esc(item.summary_title || item.title || "（无标题）");
            const url = String(item.url || "").trim();
            const source = esc(item.source || "-");
            const provider = esc(item.provider || "-");
            const tsText = fmtTs(item.published_at);
            const summarySentiment = item.summary_sentiment || "neutral";
            const sentimentCls = summarySentimentClass(summarySentiment);
            const sentimentTxt = summarySentimentText(summarySentiment);
            const titleHtml = url
                ? `<a class="news-title news-white" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${title}</a>`
                : `<span class="news-title news-white">${title}</span>`;
            return `
                <div class="list-item news-row">
                    <div class="news-main">
                        ${titleHtml}
                        <div class="news-meta">
                            <span>${tsText}</span>
                            <span class="news-tag-white">${provider}</span>
                            <span class="${sentimentCls}">${sentimentTxt}</span>
                            <span>${source}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join("");
    }

    function renderStructuredEvents(items) {
        const box = document.getElementById("news-structured-list");
        const counter = document.getElementById("news-structured-count");
        if (!box) return;
        const structured = (items || []).filter((item) => item.has_event);
        if (counter) counter.textContent = `${structured.length} 条`;
        if (!structured.length) {
            box.innerHTML = '<div class="list-item news-white">暂无已结构化事件</div>';
            return;
        }
        box.innerHTML = structured.map((item) => {
            const title = esc(item.summary_title || item.title || "（无标题）");
            const url = String(item.url || "").trim();
            const source = esc(item.source || "-");
            const provider = esc(item.provider || "-");
            const symbol = esc(item.symbol || "-");
            const eventType = esc(item.event_type || "raw");
            const impact = Number(item.impact_score || 0);
            const sentiment = Number(item.sentiment || 0);
            const tsText = fmtTs(item.published_at);
            const sentimentCls = sentimentClass(sentiment);
            const titleHtml = url
                ? `<a class="news-title news-white" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${title}</a>`
                : `<span class="news-title news-white">${title}</span>`;
            return `
                <div class="list-item news-row">
                    <div class="news-main">
                        ${titleHtml}
                        <div class="news-meta">
                            <span>${tsText}</span>
                            <span class="news-tag-white">${provider}</span>
                            <span class="news-tag-white">${symbol}</span>
                            <span class="news-tag-white">${eventType}</span>
                            <span class="${sentimentCls}">${sentimentText(sentiment)}</span>
                            <span>impact ${impact.toFixed(3)}</span>
                            <span>${source}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join("");
    }

    async function loadHealth() {
        try {
            const data = await request("/health");
            const sources = data?.sources || {};
            const active = Object.entries(sources).filter((x) => Boolean(x[1])).map((x) => x[0]);
            setHealth(true, `新闻服务正常 | 源: ${active.join(", ") || "-"}`);
        } catch (err) {
            setHealth(false, `新闻服务异常: ${err.message}`);
        }
    }

    async function loadSummary() {
        const symbol = getSymbolInput();
        const hours = getHoursInput();
        const params = new URLSearchParams({ hours: String(hours), feed_limit: String(getMaxRecordsInput()) });
        if (symbol) params.set("symbol", symbol);
        return await request(`/summary?${params.toString()}`, { timeoutMs: 20000 });
    }

    async function loadFeed(useSummarize = true) {
        const symbol = getSymbolInput();
        const hours = getHoursInput();
        const maxRecords = getMaxRecordsInput();
        const params = new URLSearchParams({
            hours: String(hours),
            limit: String(useSummarize ? Math.min(60, maxRecords) : maxRecords),
            summarize: useSummarize ? "true" : "false",
        });
        if (symbol) params.set("symbol", symbol);
        return await request(`/latest?${params.toString()}`, { timeoutMs: useSummarize ? 65000 : 25000 });
    }

    async function pullNow(silent = false) {
        if (state.pulling) return null;
        state.pulling = true;
        const output = document.getElementById("news-action-output");
        const hours = getHoursInput();
        const maxRecords = getMaxRecordsInput();
        const payload = {
            since_minutes: Math.max(30, Math.min(1440, hours * 60)),
            max_records: maxRecords,
        };
        try {
            const data = await request("/pull_now", {
                method: "POST",
                body: JSON.stringify(payload),
                timeoutMs: 120000,
            });
            if (!silent && output) output.textContent = JSON.stringify(data, null, 2);
            if (!silent) notify(`拉取完成：新增事件 ${Number(data.events_count || 0)} 条`);
            return data;
        } catch (err) {
            if (!silent && output) output.textContent = `拉取失败: ${err.message}`;
            if (!silent) notify(`拉取失败: ${err.message}`, true);
            return null;
        } finally {
            state.pulling = false;
        }
    }

    function applyNewsData(summary, latest) {
        state.summary = summary || null;
        state.latest = latest || null;
        updateSummary(summary, latest);
        renderUnstructuredNews(latest?.items || []);
        renderStructuredEvents(latest?.items || []);
        renderBucketStats(summary || {});
    }

    async function refreshAll() {
        try {
            const [summary, latest] = await Promise.all([loadSummary(), loadFeed(true), loadHealth()]);
            applyNewsData(summary, latest);
        } catch (err) {
            try {
                const [summary, latestFallback] = await Promise.all([loadSummary(), loadFeed(false), loadHealth()]);
                applyNewsData(summary, latestFallback);
                notify("新闻摘要超时，已快速回退刷新", false);
            } catch (err2) {
                notify(`刷新失败: ${err2.message || err.message}`, true);
            }
        }
    }

    function restartTimer() {
        if (state.timer) clearInterval(state.timer);
        const sec = getAutoRefreshSec();
        state.timer = setInterval(() => refreshAll(), sec * 1000);
    }

    function connectWs() {
        const proto = location.protocol === "https:" ? "wss" : "ws";
        const ws = new WebSocket(`${proto}://${location.host}/ws`);
        state.ws = ws;
        ws.onmessage = (evt) => {
            try {
                const msg = JSON.parse(evt.data || "{}");
                if (msg?.event === "news_update") refreshAll();
            } catch (err) {
                console.error(err);
            }
        };
        ws.onclose = () => setTimeout(connectWs, 2000);
    }

    function bindActions() {
        document.getElementById("news-refresh-btn")?.addEventListener("click", refreshAll);
        document.getElementById("news-pull-btn")?.addEventListener("click", async () => {
            await pullNow(false);
            await refreshAll();
        });
        document.getElementById("news-auto-refresh-sec")?.addEventListener("change", restartTimer);
        document.getElementById("news-hours")?.addEventListener("change", refreshAll);
        document.getElementById("news-symbol")?.addEventListener("change", refreshAll);
        document.getElementById("news-max-records")?.addEventListener("change", refreshAll);
        document.getElementById("news-bucket-granularity")?.addEventListener("change", () => renderBucketStats(state.summary || {}));
        document.getElementById("news-summary-granularity")?.addEventListener("change", () => updateSummary(state.summary || {}, state.latest || {}));
    }

    async function init() {
        if (!document.getElementById("news-structured-list")) return;
        setNowTime();
        setInterval(setNowTime, 1000);
        bindActions();
        restartTimer();
        connectWs();
        await refreshAll();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
