(function () {
    const API_BASE = "/api/news";
    const LIST_ID = "dashboard-news-list";
    const PULL_BTN_ID = "dashboard-news-pull-btn";
    const UPDATED_ID = "dashboard-news-updated";
    const SOURCES_ID = "dashboard-news-sources";
    const REFRESH_MS = 15000;
    let loading = false;
    let needsRefresh = false;

    function esc(value) {
        return String(value ?? "").replace(/[&<>"']/g, (m) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
        }[m]));
    }

    function plainText(value) {
        return String(value ?? "")
            .replace(/<\s*br\s*\/?>/gi, " ")
            .replace(/<[^>]+>/g, " ")
            .replace(/\s+/g, " ")
            .trim();
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

    function isDashboardVisible() {
        const host = document.getElementById(LIST_ID);
        if (!host) return false;
        const tab = document.getElementById("dashboard");
        return !!tab && tab.classList.contains("active");
    }

    function canRunDashboardPolling() {
        if (typeof window === "undefined" || typeof window.__ctsSharedPolling?.canRun !== "function") return true;
        return window.__ctsSharedPolling.canRun("dashboard");
    }

    async function request(path, options = {}) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), Math.max(5000, Number(options.timeoutMs || 18000)));
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
            if (!response.ok) {
                throw new Error(payload.detail || payload.error || `request failed (${response.status})`);
            }
            return payload;
        } catch (e) {
            if (e?.name === "AbortError") throw new Error(`请求超时: ${path}`);
            throw e;
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
            return `<div class="list-item"><span>${esc(plainText(name))}</span><span>${Number(count)} (${pct.toFixed(1)}%)</span></div>`;
        }).join("");
    }

    function renderNews(items) {
        const box = document.getElementById(LIST_ID);
        if (!box) return;
        if (!Array.isArray(items) || !items.length) {
            box.innerHTML = '<div class="list-item">暂无新闻，等待后台拉取</div>';
            return;
        }

        box.innerHTML = items.map((item) => {
            const title = esc(plainText(item.summary_title || item.title || "（无标题）"));
            const url = String(item.url || "").trim();
            const source = esc(plainText(item.source || "-"));
            const provider = esc(plainText(item.provider || "-"));
            const symbol = esc(plainText(item.symbol || "-"));
            const eventType = esc(plainText(item.event_type || "raw"));
            const impact = Number(item.impact_score || 0);
            const hasEvent = Boolean(item.has_event);
            const sentiment = hasEvent
                ? Number(item.sentiment || 0)
                : (item.summary_sentiment === "positive" ? 1 : item.summary_sentiment === "negative" ? -1 : 0);
            const tsText = fmtTs(item.published_at);
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
                            <span class="${sentimentClass(sentiment)}">${sentimentText(sentiment)}</span>
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

    async function loadNews(force = false) {
        if (!force && (document.hidden || !isDashboardVisible())) {
            needsRefresh = true;
            return;
        }
        if (!force && !canRunDashboardPolling()) {
            needsRefresh = true;
            return;
        }
        if (loading) {
            if (force) needsRefresh = true;
            return;
        }
        loading = true;
        needsRefresh = false;
        try {
            let data;
            try {
                data = await request("/latest?limit=10&hours=24&summarize=true", { timeoutMs: 25000 });
            } catch (_) {
                data = await request("/latest?limit=10&hours=24&summarize=false", { timeoutMs: 25000 });
            }
            renderNews(data.items || []);
            renderSourceStats(data.source_stats || {});
            setUpdatedText(new Date().toISOString());
        } catch (err) {
            const box = document.getElementById(LIST_ID);
            if (box) box.innerHTML = `<div class="list-item">新闻加载失败: ${esc(err.message)}</div>`;
        } finally {
            loading = false;
            if (needsRefresh && !document.hidden && isDashboardVisible()) {
                needsRefresh = false;
                setTimeout(() => { loadNews(false); }, 80);
            }
        }
    }

    async function pullNow() {
        const btn = document.getElementById(PULL_BTN_ID);
        if (!btn) return;
        btn.disabled = true;
        try {
            const data = await request("/pull_now?background=true", {
                method: "POST",
                timeoutMs: 20000,
                body: JSON.stringify({
                    since_minutes: 240,
                    max_records: 120,
                }),
            });
            if (Number(data?.queued_count || data?.job?.result?.queued_count || 0) > 0) {
                request("/worker/run_once?llm_limit=8&background=true", { method: "POST", timeoutMs: 8000 }).catch(() => ({}));
            }
            if (typeof window.notify === "function") {
                window.notify(`新闻任务已启动：新增原始 ${Number(data.raw_inserted_count || 0)} 条，结构化处理中`);
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
        document.querySelectorAll('.tab-btn[data-tab="dashboard"]').forEach((tabBtn) => tabBtn.addEventListener("click", () => {
            setTimeout(() => {
                if (isDashboardVisible()) loadNews(true);
            }, 120);
        }));
        document.addEventListener("visibilitychange", () => {
            if (document.hidden) {
                needsRefresh = true;
                return;
            }
            if (isDashboardVisible()) loadNews(false);
        });
    }

    function start() {
        if (!document.getElementById(LIST_ID)) return;
        bindActions();
        if (isDashboardVisible()) loadNews(true);
        else needsRefresh = true;
        setInterval(() => {
            if (document.hidden || !isDashboardVisible()) {
                needsRefresh = true;
                return;
            }
            loadNews(false);
        }, REFRESH_MS);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", start);
    } else {
        start();
    }
})();
