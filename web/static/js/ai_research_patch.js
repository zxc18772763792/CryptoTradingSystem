(function () {
    const API = "/api";
    const state = { timer: null };

    const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));

    function notify(msg, isError = false) {
        if (typeof window.notify === "function") {
            window.notify(msg, isError);
        }
    }

    function parseTs(value) {
        if (!value) return null;
        let text = String(value).trim();
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(text)) text += "Z";
        const date = new Date(text);
        return Number.isFinite(date.getTime()) ? date : null;
    }

    function fmtTs(value) {
        const date = parseTs(value);
        return date ? date.toLocaleString("zh-CN", { hour12: false }) : "--";
    }

    async function api(path, options = {}) {
        const controller = new AbortController();
        const timeoutMs = Math.max(3000, Number(options.timeoutMs || 12000));
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const sep = path.includes("?") ? "&" : "?";
        try {
            const response = await fetch(`${API}${path}${sep}_ts=${Date.now()}`, {
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
        } catch (err) {
            if (err?.name === "AbortError") throw new Error(`请求超时(${timeoutMs}ms): ${path}`);
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    function currentSymbol() {
        const planner = String(document.getElementById("ai-planner-symbols")?.value || "").split(",").map((s) => s.trim()).filter(Boolean)[0];
        const fallback = String(document.getElementById("research-symbol")?.value || document.getElementById("data-symbol")?.value || "BTC/USDT").trim();
        return planner || fallback || "BTC/USDT";
    }

    function currentExchange() {
        return String(document.getElementById("run-exchange")?.value || document.getElementById("research-exchange")?.value || document.getElementById("data-exchange")?.value || "binance").trim() || "binance";
    }

    function newsKey(symbol) {
        const raw = String(symbol || "").trim().toUpperCase();
        const main = raw.split(":")[0];
        if (main.includes("/")) return main.split("/")[0];
        return main.replace(/(USDT|USDC|FDUSD|BUSD|USD)$/i, "") || main;
    }

    async function refreshDataReadiness() {
        const summaryEl = document.getElementById("ai-data-readiness-summary");
        const detailsEl = document.getElementById("ai-data-readiness-details");
        if (!summaryEl || !detailsEl) return;

        const exchange = currentExchange();
        const symbol = currentSymbol();
        const symbolKey = newsKey(symbol);
        summaryEl.textContent = "正在刷新新闻/宏观/社区诊断...";

        const [healthRes, scopedRes, globalRes, workerRes, fundingRes, microRes, communityRes] = await Promise.allSettled([
            api("/news/health", { timeoutMs: 10000 }),
            api(`/news/brief?symbol=${encodeURIComponent(symbolKey)}&hours=24&feed_limit=40`, { timeoutMs: 10000 }),
            api("/news/brief?hours=24&feed_limit=40", { timeoutMs: 10000 }),
            api("/news/worker_status", { timeoutMs: 10000 }),
            api(`/ai/diagnostics/funding-cache?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&days=60`, { timeoutMs: 12000 }),
            api(`/trading/analytics/microstructure?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&depth_limit=20`, { timeoutMs: 12000 }),
            api(`/trading/analytics/community/overview?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}`, { timeoutMs: 12000 }),
        ]);

        const health = healthRes.status === "fulfilled" ? (healthRes.value || {}) : {};
        const scoped = scopedRes.status === "fulfilled" ? (scopedRes.value || {}) : {};
        const global = globalRes.status === "fulfilled" ? (globalRes.value || {}) : {};
        const worker = workerRes.status === "fulfilled" ? (workerRes.value || {}) : {};
        const funding = fundingRes.status === "fulfilled" ? (fundingRes.value?.funding || {}) : {};
        const micro = microRes.status === "fulfilled" ? (microRes.value || {}) : {};
        const community = communityRes.status === "fulfilled" ? (communityRes.value || {}) : {};

        const active = (Number(scoped?.events_count || 0) > 0 || Number(scoped?.feed_count || 0) > 0) ? scoped : global;
        const scopeText = active === scoped ? `当前币种 ${symbolKey}` : "全局回退";
        const enabledSources = Object.entries(health?.sources || {}).filter(([, enabled]) => !!enabled).length;
        const sourceStates = health?.source_states || worker?.source_states || [];
        const llmQueue = health?.llm_queue || worker?.llm_queue || {};
        const fundingRows = Number(funding?.rows || 0);
        const fundingRate = Number(micro?.funding_rate?.funding_rate);
        const basisPct = Number(micro?.spot_futures_basis?.basis_pct);
        const whaleCount = Number(community?.whale_transfers?.count || 0);
        const announcementCount = Array.isArray(community?.announcements) ? community.announcements.length : 0;
        const issues = [];
        if (!Number(active?.raw_count || 0) && !Number(active?.feed_count || 0)) issues.push("新闻窗口为空");
        if (Number(llmQueue?.pending_total || 0) > 0) issues.push(`LLM 队列待处理 ${Number(llmQueue?.pending_total || 0)} 条`);
        if (!fundingRows) issues.push("资金费率缓存为空");
        if (!Number.isFinite(fundingRate)) issues.push("Funding 实时值缺失");
        if (!whaleCount && !announcementCount) issues.push("社区/巨鲸快照为空");

        summaryEl.textContent = issues.length ? `待补齐: ${issues.join(" / ")}` : "新闻、宏观和社区层已就绪";

        detailsEl.innerHTML = [
            `<div style="padding:8px;background:#141f2f;border-radius:6px;"><div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">新闻诊断</div><div>口径: ${esc(scopeText)} / 原始 ${Number(active?.raw_count || 0)} / Feed ${Number(active?.feed_count || 0)} / 事件 ${Number(active?.events_count || 0)}</div><div>启用源 ${enabledSources} / 源状态 ${sourceStates.length} / LLM 排队 ${Number(llmQueue?.pending_total || 0)}</div><div>最近原始 / 事件: ${fmtTs(active?.latest_raw_at)} / ${fmtTs(active?.latest_event_at)}</div></div>`,
            `<div style="padding:8px;background:#141f2f;border-radius:6px;"><div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">宏观 / 资金费率</div><div>缓存行数 ${fundingRows} / Funding ${Number.isFinite(fundingRate) ? fundingRate.toFixed(6) : "--"} / Basis ${Number.isFinite(basisPct) ? `${basisPct.toFixed(3)}%` : "--"}</div><div>覆盖区间 ${esc(funding?.coverage?.start || "--")} ~ ${esc(funding?.coverage?.end || "--")}</div><div style="margin-top:4px;color:#7e92b2;">缓存路径: ${esc(funding?.cache_path || "--")}</div></div>`,
            `<div style="padding:8px;background:#141f2f;border-radius:6px;"><div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">社区 / 巨鲸 / 微观结构</div><div>巨鲸 ${whaleCount} / 公告 ${announcementCount} / 盘口价差 ${Number(micro?.orderbook?.spread_bps || 0).toFixed(2)} bps</div><div>最近 LLM 批次: ${fmtTs(worker?.last_llm_batch?.timestamp)} / 最近拉取: ${fmtTs(worker?.last_pull?.timestamp)}</div></div>`,
            `<div style="padding:8px;background:#141f2f;border-radius:6px;"><div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">存储位置</div><div style="margin-top:3px;color:#9fb1c9;">新闻数据库: ./data/crypto_trading.db</div><div style="margin-top:3px;color:#9fb1c9;">Funding 缓存: ${esc(funding?.cache_path || "--")}</div></div>`,
        ].join("");
    }

    async function pullNewsForResearch() {
        const symbol = currentSymbol();
        const result = await api("/news/pull_now?background=true", {
            method: "POST",
            body: JSON.stringify({ since_minutes: 720, max_records: 120, query: newsKey(symbol) }),
            timeoutMs: 15000,
        });
        const queued = Number(result?.queued_count || result?.job?.result?.queued_count || 0);
        if (queued > 0) {
            api("/news/worker/run_once?llm_limit=8&background=true", { method: "POST", timeoutMs: 6000 }).catch(() => ({}));
        }
        notify(result?.queued ? "新闻拉取任务已加入后台" : "新闻拉取已完成");
        setTimeout(() => refreshDataReadiness().catch(() => {}), 1200);
    }

    async function retireProposal(proposalId) {
        if (!proposalId) return;
        if (!window.confirm(`确认退役该研究方案？\n${proposalId}\n退役后即可删除。`)) return;
        try {
            await api(`/ai/proposals/${encodeURIComponent(proposalId)}/retire`, {
                method: "POST",
                body: JSON.stringify({ notes: "retired from AI research queue" }),
                timeoutMs: 15000,
            });
            notify("研究方案已退役");
        } catch (err) {
            const msg = String(err?.message || "");
            if (/404|not found/i.test(msg)) {
                await api(`/ai/proposals/${encodeURIComponent(proposalId)}`, { method: "DELETE", timeoutMs: 15000 });
                notify("旧影子记录未命中退役接口，已直接删除");
            } else {
                throw err;
            }
        }
        if (window.AI?.refreshWorkbench) {
            window.AI.refreshWorkbench().catch(() => {});
        }
        setTimeout(() => refreshDataReadiness().catch(() => {}), 1200);
    }

    function handleCaptureClick(event) {
        const refreshBtn = event.target.closest("#ai-data-refresh-btn");
        if (refreshBtn) {
            event.preventDefault();
            event.stopImmediatePropagation();
            refreshDataReadiness().catch((err) => notify(`数据诊断刷新失败: ${err.message}`, true));
            return;
        }
        const newsBtn = event.target.closest("#ai-news-pull-btn");
        if (newsBtn) {
            event.preventDefault();
            event.stopImmediatePropagation();
            pullNewsForResearch().catch((err) => notify(`新闻拉取失败: ${err.message}`, true));
            return;
        }
        const retireBtn = event.target.closest('[data-action="retire-proposal"][data-proposal-id]');
        if (retireBtn) {
            event.preventDefault();
            event.stopImmediatePropagation();
            retireProposal(String(retireBtn.dataset.proposalId || "").trim()).catch((err) => notify(`退役失败: ${err.message}`, true));
        }
    }

    function init() {
        if (!document.getElementById("ai-data-readiness-summary")) return;
        document.addEventListener("click", handleCaptureClick, true);
        document.querySelector('.tab-btn[data-tab="ai-research"]')?.addEventListener("click", () => {
            setTimeout(() => refreshDataReadiness().catch(() => {}), 120);
        });
        clearInterval(state.timer);
        state.timer = setInterval(() => refreshDataReadiness().catch(() => {}), 30000);
        setTimeout(() => refreshDataReadiness().catch(() => {}), 1200);
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
    else init();
})();
