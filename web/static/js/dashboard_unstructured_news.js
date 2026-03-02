(function () {
    const API_BASE = "/api/news";
    const LIST_ID = "dashboard-unstructured-list";
    const COUNT_ID = "dashboard-unstructured-count";
    const REFRESH_BTN_ID = "dashboard-unstructured-refresh";
    const BUCKET_GRAN_ID = "dashboard-news-bucket-granularity";
    const BUCKET_CHART_ID = "dashboard-news-bucket-chart";
    const REFRESH_MS = 30000;
    let loading = false;

    function esc(value) {
        return String(value ?? "").replace(/[&<>"']/g, (m) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
        }[m]));
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

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), Math.max(5000, Number(options.timeoutMs || 15000)));
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
            if (!response.ok) throw new Error(payload.detail || payload.error || `请求失败(${response.status})`);
            return payload;
        } catch (e) {
            if (e?.name === "AbortError") throw new Error(`请求超时: ${path}`);
            throw e;
        } finally {
            clearTimeout(timer);
        }
    }

    function renderUnstructuredNews(items) {
        const box = document.getElementById(LIST_ID);
        const countEl = document.getElementById(COUNT_ID);
        if (!box) return;
        const unstructured = (items || []).filter((item) => !item.has_event);
        if (countEl) countEl.textContent = `未结构化: ${unstructured.length}`;
        if (!unstructured.length) {
            box.innerHTML = '<div class="list-item">暂无未结构化新闻</div>';
            return;
        }
        box.innerHTML = unstructured.slice(0, 6).map((item) => {
            const title = esc(item.summary_title || item.title || "（无标题）");
            const url = String(item.url || "").trim();
            const provider = esc(item.provider || "-");
            const tsText = fmtTs(item.published_at);
            const summarySentiment = item.summary_sentiment || "neutral";
            const sentimentCls = summarySentimentClass(summarySentiment);
            const sentimentTxt = summarySentimentText(summarySentiment);
            const titleHtml = url
                ? `<a class="news-title" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${title}</a>`
                : `<span class="news-title">${title}</span>`;

            return `
                <div class="list-item news-row">
                    <div class="news-main">
                        ${titleHtml}
                        <div class="news-meta">
                            <span>${tsText}</span>
                            <span class="news-tag">${provider}</span>
                            <span class="${sentimentCls}">${sentimentTxt}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join("");
    }

    function renderBucketSpark(summary) {
        const el = document.getElementById(BUCKET_CHART_ID);
        if (!el) return;
        const gran = String(document.getElementById(BUCKET_GRAN_ID)?.value || "1h");
        const buckets = summary?.bucket_stats?.[gran] || [];
        if (!buckets.length) {
            el.innerHTML = '<div class="list-item" style="padding:8px;">暂无结构化事件统计</div>';
            return;
        }
        const recent = buckets.slice(-16);
        if (typeof Plotly === "undefined") {
            el.innerHTML = `<div class="list-item" style="padding:8px;">结构化事件活跃度（${gran}）：${recent.map(x => Number(x.count || 0)).join(" / ")}</div>`;
            return;
        }
        const draw = () => {
            if (!el || el.offsetWidth < 30 || el.offsetHeight < 30) {
                setTimeout(() => renderBucketSpark(summary), 180);
                return;
            }
            el.innerHTML = "";
            const x = recent.map((row) => parseTs(row.bucket_start) || row.bucket_start);
            const total = recent.map((row) => Number(row.count || 0));
            const pos = recent.map((row) => Number(row.positive || 0));
            const neg = recent.map((row) => Number(row.negative || 0));
            try {
                if (typeof Plotly.purge === "function") Plotly.purge(el);
                Plotly.react(
                    el,
                    [
                        { type: "bar", x, y: total, name: "总数", marker: { color: "#1f9d63", opacity: 0.35 } },
                        { type: "scatter", mode: "lines+markers", x, y: pos, name: "正面", line: { color: "#20bf78", width: 1.6 }, marker: { size: 4 } },
                        { type: "scatter", mode: "lines+markers", x, y: neg, name: "负面", line: { color: "#ea5b61", width: 1.6 }, marker: { size: 4 } },
                    ],
                    {
                        paper_bgcolor: "#111723",
                        plot_bgcolor: "#111723",
                        font: { color: "#d7dde8", size: 10 },
                        margin: { l: 24, r: 12, t: 8, b: 20 },
                        xaxis: { showgrid: false, tickfont: { size: 10 }, automargin: true },
                        yaxis: { showgrid: true, gridcolor: "#283242", rangemode: "tozero", tickfont: { size: 10 }, automargin: true },
                        legend: { orientation: "h", x: 0, y: 1.18, font: { size: 10 } },
                        barmode: "overlay",
                        hovermode: "x unified",
                    },
                    { responsive: true, displaylogo: false }
                );
                setTimeout(() => { try { Plotly.Plots.resize(el); } catch (_) {} }, 50);
                setTimeout(() => { try { Plotly.Plots.resize(el); } catch (_) {} }, 250);
            } catch (e) {
                el.innerHTML = `<div class="list-item" style="padding:8px;">图表渲染失败: ${esc(e?.message || e)}</div>`;
            }
        };
        requestAnimationFrame(draw);
    }

    async function loadSummary() {
        return request("/summary?hours=24&feed_limit=60", { timeoutMs: 12000 });
    }

    async function loadNews() {
        if (loading) return;
        loading = true;
        try {
            const [data, summary] = await Promise.all([
                request("/latest?limit=40&hours=24&summarize=false", { timeoutMs: 12000 }),
                loadSummary().catch(() => null),
            ]);
            renderUnstructuredNews(data.items || []);
            renderBucketSpark(summary);
        } catch (err) {
            const box = document.getElementById(LIST_ID);
            if (box) box.innerHTML = `<div class="list-item">加载失败: ${esc(err.message)}</div>`;
        } finally {
            loading = false;
        }
    }

    function bindActions() {
        document.getElementById(REFRESH_BTN_ID)?.addEventListener("click", loadNews);
        document.getElementById(BUCKET_GRAN_ID)?.addEventListener("change", loadNews);
    }

    function connectWs() {
        try {
            const proto = location.protocol === "https:" ? "wss" : "ws";
            const ws = new WebSocket(`${proto}://${location.host}/ws`);
            ws.onmessage = (evt) => {
                try {
                    const msg = JSON.parse(evt.data || "{}");
                    if (msg?.event === "news_update") loadNews();
                } catch (_) {}
            };
            ws.onclose = () => setTimeout(connectWs, 2000);
        } catch (_) {}
    }

    async function start() {
        if (!document.getElementById(LIST_ID)) return;
        bindActions();
        connectWs();
        await loadNews();
        setInterval(loadNews, REFRESH_MS);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", start);
    } else {
        start();
    }
})();
