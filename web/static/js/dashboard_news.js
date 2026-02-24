(function () {
    const API_BASE = "/api/news";
    const LIST_ID = "dashboard-news-list";
    const PULL_BTN_ID = "dashboard-news-pull-btn";
    const UPDATED_ID = "dashboard-news-updated";
    const SOURCES_ID = "dashboard-news-sources";
    const REFRESH_MS = 15000;
    let loading = false;

    function esc(value) {
        return String(value ?? "").replace(/[&<>"']/g, (m) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            "\"": "&quot;",
            "'": "&#39;",
        }[m]));
    }

    function parseTs(value) {
        if (!value) return null;
        const d = new Date(String(value));
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
        if (Number(sentiment) > 0) return "\u6b63\u9762";
        if (Number(sentiment) < 0) return "\u8d1f\u9762";
        return "\u4e2d\u6027";
    }

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), Math.max(5000, Number(options.timeoutMs || 18000)));
        try {
            const response = await fetch(`${API_BASE}${path}`, {
                ...options,
                signal: controller.signal,
                headers: {
                    "Content-Type": "application/json",
                    ...(options.headers || {}),
                },
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                throw new Error(payload.detail || payload.error || `request failed (${response.status})`);
            }
            return payload;
        } finally {
            clearTimeout(timer);
        }
    }

    function renderSourceStats(sourceStats) {
        const box = document.getElementById(SOURCES_ID);
        if (!box) return;
        const byProvider = sourceStats?.by_provider || {};
        const rows = Object.entries(byProvider).slice(0, 6);
        if (!rows.length) {
            box.innerHTML = '<div class="list-item">暂无来源统计</div>';
            return;
        }
        const total = rows.reduce((acc, row) => acc + Number(row[1] || 0), 0);
        box.innerHTML = rows.map(([name, count]) => {
            const pct = total > 0 ? (Number(count) / total) * 100 : 0;
            return `<div class="list-item"><span>${esc(name)}</span><span>${Number(count)} (${pct.toFixed(1)}%)</span></div>`;
        }).join("");
    }

    function renderNews(items) {
        const box = document.getElementById(LIST_ID);
        if (!box) return;
        if (!Array.isArray(items) || !items.length) {
            box.innerHTML = '<div class="list-item">暂无新闻，等待后台拉�?..</div>';
            return;
        }

        box.innerHTML = items.map((item) => {
            const title = esc(item.title || "\uFF08\u65E0\u6807\u9898\uFF09");
            const url = String(item.url || "").trim();
            const source = esc(item.source || "-");
            const provider = esc(item.provider || "-");
            const symbol = esc(item.symbol || "-");
            const eventType = esc(item.event_type || "raw");
            const impact = Number(item.impact_score || 0);
            const sentiment = Number(item.sentiment || 0);
            const hasEvent = Boolean(item.has_event);
            const tsText = fmtTs(item.published_at);
            const sentimentCls = sentimentClass(sentiment);
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
                            <span class="news-tag">${symbol}</span>
                            <span class="news-tag">${eventType}</span>
                            <span class="${sentimentCls}">${sentimentText(sentiment)}</span>
                            ${hasEvent ? `<span>impact ${impact.toFixed(3)}</span>` : "<span>未结构化</span>"}
                            <span>${source}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join("");
    }

    function setUpdatedText(value) {
        const el = document.getElementById(UPDATED_ID);
        if (!el) return;
        el.textContent = `最近更新：${fmtTs(value || new Date().toISOString())}`;
    }

    async function loadNews() {
        if (loading) return;
        loading = true;
        try {
            const data = await request("/latest?limit=10&hours=24");
            renderNews(data.items || []);
            renderSourceStats(data.source_stats || {});
            setUpdatedText(new Date().toISOString());
        } catch (err) {
            const box = document.getElementById(LIST_ID);
            if (box) box.innerHTML = `<div class="list-item">新闻加载失败�?{esc(err.message)}</div>`;
        } finally {
            loading = false;
        }
    }

    async function pullNow() {
        const btn = document.getElementById(PULL_BTN_ID);
        if (!btn) return;
        btn.disabled = true;
        try {
            const data = await request("/pull_now", {
                method: "POST",
                timeoutMs: 120000,
                body: JSON.stringify({
                    since_minutes: 240,
                    max_records: 120,
                }),
            });
            if (typeof window.notify === "function") {
                window.notify(`新闻已拉取：新增事件 ${Number(data.events_count || 0)} 条`);
            }
            await loadNews();
        } catch (err) {
            if (typeof window.notify === "function") {
                window.notify(`新闻拉取失败: ${err.message}`, true);
            }
        } finally {
            btn.disabled = false;
        }
    }

    function bindActions() {
        const btn = document.getElementById(PULL_BTN_ID);
        if (btn) btn.addEventListener("click", pullNow);
    }

    function start() {
        if (!document.getElementById(LIST_ID)) return;
        bindActions();
        loadNews();
        setInterval(loadNews, REFRESH_MS);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", start);
    } else {
        start();
    }
})();
