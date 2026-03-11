# AI研究→决策一条龙 — 最小改动实施计划

> **原则**：不动现有核心逻辑（backtest_engine、strategy_manager、risk_manager），
> 只在 API 层加薄接口、在 JS 层加 UI 交互。每个 Phase 独立可部署，不依赖前一个 Phase 完成。

---

## 背景：已有的关键组件

| 组件 | 位置 | 状态 |
|---|---|---|
| `SignalAggregator` | `core/ai/signal_aggregator.py` | 已完成，`aggregate(symbol, df)` → `AggregatedSignal.to_dict()` |
| `RiskGate` | `core/ai/risk_gate.py` | 已集成进 `SignalAggregator._apply_risk_gate` |
| `CUSUMWatcher` | `core/monitoring/cusum_watcher.py` | 每5分钟运行，`_demote_on_decay` |
| 人工审批 API | `web/api/ai_research.py` | `GET /candidates/pending-approvals`, `POST /candidates/{id}/human-approve` |
| 候选注册 API | `web/api/ai_research.py` | `POST /candidates/{id}/register` (mode=paper/live_candidate) |
| 性能快照 | `config/database.py` + `web/api/ai_research.py` | `StrategyPerformanceSnapshot` 表，`/performance/snapshots` |
| `PnLDecomposer` | `core/accounting/pnl_decomposer.py` | 已完成 |

---

## Phase A — 实时信号可见性（只读，零风险）

**目标**：在 AI 研究 Hub 右侧面板展示 `paper_running`/`live_running` 候选的实时三路信号

### A1. 新增 API 端点

**文件**：`web/api/ai_research.py`

在文件末尾（`router` 部分）添加一个新的端点，**不改动任何现有函数**：

```python
# ── 在文件末尾添加，不改动其他任何内容 ──

@router.get("/candidates/live-signals")
async def get_live_signals(request: Request, symbol: Optional[str] = None):
    """Return SignalAggregator output for all paper_running/live_running candidates.

    Uses lazy singleton aggregator stored in app.state to avoid re-loading ML model.
    Market data is fetched from strategy_manager cache (non-blocking, 1h timeframe).
    If market data unavailable, returns signal with empty DataFrame (LLM-only).
    """
    ensure_ai_research_runtime_state(request.app)

    # Lazy singleton: create once, reuse across calls
    if not hasattr(request.app.state, "_signal_aggregator"):
        from core.ai.signal_aggregator import SignalAggregator
        request.app.state._signal_aggregator = SignalAggregator()

    aggregator = request.app.state._signal_aggregator

    all_candidates = list_candidates(request.app, limit=200)
    active = [
        c for c in all_candidates
        if str(c.status) in {"paper_running", "shadow_running", "live_running", "live_candidate"}
    ]

    # Filter by symbol if requested
    if symbol:
        sym_norm = _normalize_symbol(symbol)
        active = [c for c in active if _normalize_symbol(c.symbols[0] if c.symbols else "") == sym_norm]

    results = []
    for cand in active:
        cand_symbol = cand.symbols[0] if cand.symbols else "BTC/USDT"
        try:
            import pandas as pd
            # Try to get cached market data from strategy_manager (non-blocking)
            df = pd.DataFrame()
            try:
                from core.strategies import strategy_manager as sm
                df = sm._load_market_data(cand_symbol, "1h")  # uses existing 30s TTL cache
            except Exception:
                pass  # proceed with empty df — LLM signal still works

            sig = await aggregator.aggregate(cand_symbol, df)
            results.append({
                "candidate_id": cand.candidate_id,
                "strategy": cand.strategy,
                "symbol": cand_symbol,
                "status": str(cand.status),
                "signal": sig.to_dict(),
            })
        except Exception as exc:
            logger.debug(f"live-signals: error for {cand.candidate_id}: {exc}")
            results.append({
                "candidate_id": cand.candidate_id,
                "strategy": cand.strategy,
                "symbol": cand_symbol,
                "status": str(cand.status),
                "signal": None,
                "error": str(exc),
            })

    return {"items": results, "count": len(results), "ts": datetime.now(timezone.utc).isoformat()}
```

**注意**：`sm._load_market_data` 已有 30s TTL LRU 缓存（2026-03-03 加的），不会发起额外网络请求。

### A2. 前端轮询展示

**文件**：`web/static/js/ai_research.js`

找到 `refreshWorkbench()` 函数（已存在），在函数体末尾添加一行调用：
```javascript
// 在 refreshWorkbench() 末尾添加（不改动已有逻辑）
loadLiveSignals();
```

在文件末尾追加新函数（不替换任何现有函数）：
```javascript
// ── 实时信号面板 ──
let _liveSignalTimer = null;

async function loadLiveSignals() {
    try {
        const r = await apiFetch('/api/ai/candidates/live-signals');
        renderLiveSignalPanel(r.items || []);
    } catch(e) { /* silent */ }
}

function renderLiveSignalPanel(items) {
    const el = document.getElementById('ai-live-signals-panel');
    if (!el) return;
    if (!items.length) {
        el.innerHTML = '<div class="text-muted" style="font-size:12px;padding:8px">暂无运行中候选</div>';
        return;
    }
    const dirIcon = d => d === 'LONG' ? '▲' : d === 'SHORT' ? '▼' : '─';
    const dirColor = d => d === 'LONG' ? '#4ade80' : d === 'SHORT' ? '#f87171' : '#94a3b8';
    el.innerHTML = items.map(item => {
        const sig = item.signal;
        if (!sig) return `<div class="live-sig-row error">${item.strategy} — 信号错误</div>`;
        const comp = sig.components || {};
        const pct = v => (v * 100).toFixed(0) + '%';
        const blocked = sig.blocked_by_risk
            ? `<span class="sig-badge blocked" title="${sig.risk_reason}">风控拦截</span>` : '';
        const approval = sig.requires_approval && !sig.blocked_by_risk
            ? `<span class="sig-badge approval">待审批</span>` : '';
        return `
<div class="live-sig-row" data-cid="${item.candidate_id}">
  <div class="live-sig-header">
    <span class="live-sig-name">${item.strategy}</span>
    <span class="live-sig-symbol text-muted">${item.symbol}</span>
    <span class="live-sig-dir" style="color:${dirColor(sig.direction)}">${dirIcon(sig.direction)} ${sig.direction}</span>
    ${blocked}${approval}
  </div>
  <div class="live-sig-bars">
    ${['llm','ml','factor'].map(k => {
        const c = comp[k] || {};
        return `<span class="sig-bar-label">${k.toUpperCase()}</span>
                <span class="sig-bar-dir" style="color:${dirColor(c.direction)}">${dirIcon(c.direction||'FLAT')}</span>
                <span class="sig-bar-conf">${pct(c.confidence||0)}</span>`;
    }).join('')}
    <span class="sig-bar-label" style="margin-left:6px">综合</span>
    <span class="sig-bar-conf" style="font-weight:600">${pct(sig.confidence)}</span>
  </div>
</div>`;
    }).join('');
}

// 启动30秒轮询
function startLiveSignalPolling() {
    if (_liveSignalTimer) return;
    loadLiveSignals();
    _liveSignalTimer = setInterval(loadLiveSignals, 30000);
}
```

在 `DOMContentLoaded` 或页面初始化处（已有的 `initAiResearch()` 或类似函数末尾）追加：
```javascript
startLiveSignalPolling();
```

**文件**：`web/templates/index.html`

在 AI 研究 Hub 右侧面板（`#ai-detail-panel` 或类似 ID）顶部插入：
```html
<!-- 在右侧详情面板顶部，现有内容之前 -->
<div id="ai-live-signals-panel" class="ai-live-signals-panel">
  <div class="text-muted" style="font-size:12px;padding:8px">加载中...</div>
</div>
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* Phase A — 实时信号面板 */
.ai-live-signals-panel { border-bottom: 1px solid var(--border); margin-bottom: 10px; padding-bottom: 8px; }
.live-sig-row { padding: 6px 8px; border-radius: 6px; margin-bottom: 4px; background: var(--bg-2); }
.live-sig-row:hover { background: var(--bg-3); }
.live-sig-header { display: flex; align-items: center; gap: 8px; font-size: 12px; margin-bottom: 4px; }
.live-sig-name { font-weight: 600; }
.live-sig-symbol { font-size: 11px; }
.live-sig-dir { font-weight: 700; font-size: 13px; margin-left: auto; }
.live-sig-bars { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--text-muted); flex-wrap: wrap; }
.sig-bar-label { background: var(--bg-3); padding: 1px 4px; border-radius: 3px; }
.sig-bar-conf { min-width: 28px; text-align: right; }
.sig-bar-dir { font-size: 10px; }
.sig-badge { font-size: 10px; padding: 1px 5px; border-radius: 8px; }
.sig-badge.blocked { background: #7f1d1d; color: #fca5a5; }
.sig-badge.approval { background: #78350f; color: #fcd34d; }
```

---

## Phase B — 快速注册 + 仓位分配

**目标**：审批队列卡片上加"一键启动纸盘"按钮，带默认 5% 仓位分配

### B1. 新增快捷注册端点

**文件**：`web/api/ai_research.py`

在文件末尾（紧接 Phase A 端点之后）追加：

```python
class AIQuickRegisterRequest(BaseModel):
    mode: str = "paper"
    allocation_pct: float = Field(default=0.05, ge=0.001, le=1.0)

@router.post("/candidates/{candidate_id}/quick-register")
async def quick_register_candidate(
    request: Request,
    candidate_id: str,
    payload: AIQuickRegisterRequest,
):
    """One-click register with allocation stored in metadata.

    Thin wrapper around existing /candidates/{id}/register endpoint logic.
    Stores allocation_pct in candidate.metadata for future PositionSizer use.
    """
    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    # Reuse existing promote logic
    result = await promote_existing_candidate(
        request.app,
        candidate_id=candidate_id,
        target=payload.mode,
        actor="quick_register",
    )

    # Store allocation preference in metadata (non-blocking)
    cand = get_candidate(request.app, candidate_id)
    cand.metadata["allocation_pct"] = float(payload.allocation_pct)
    request.app.state.ai_candidate_registry.save(cand)

    return {**result, "allocation_pct": payload.allocation_pct}
```

### B2. 审批卡片添加快捷按钮

**文件**：`web/static/js/ai_research.js`

找到 `renderApprovalQueue()` 函数（已存在），在每张审批卡片 HTML 的按钮区域末尾插入一个新按钮：

```javascript
// 在审批卡片按钮区域追加（不改动现有 humanApprove/humanReject 按钮）
<button class="btn btn-sm btn-success" onclick="quickRegister('${c.candidate_id}', 0.05)">
  一键纸盘 5%
</button>
```

在文件末尾追加新函数：
```javascript
async function quickRegister(candidateId, allocationPct = 0.05) {
    if (!confirm(`确认注册候选 ${candidateId.slice(0,8)} 为纸盘交易，分配 ${(allocationPct*100).toFixed(0)}% 仓位？`)) return;
    try {
        const r = await apiFetch(`/api/ai/candidates/${candidateId}/quick-register`, {
            method: 'POST',
            body: JSON.stringify({ mode: 'paper', allocation_pct: allocationPct }),
        });
        showToast(`已注册为纸盘，仓位 ${(allocationPct*100).toFixed(0)}%`);
        refreshWorkbench();
    } catch(e) {
        showToast(`注册失败: ${e.message}`, 'error');
    }
}
```

---

## Phase C — CUSUM 衰减自动触发新研究

**目标**：策略 CUSUM 衰减降级后，自动生成一个 `draft` 状态的新研究提案，供用户二次确认

**改动量极小**：只在 `cusum_watcher.py` 的 `_demote_on_decay` 末尾加 ~10 行

### C1. 修改 `_demote_on_decay`

**文件**：`core/monitoring/cusum_watcher.py`

找到 `_demote_on_decay` 函数，在 `return target` 之前（两个分支各自）添加自动起草逻辑：

```python
# 在每个 return target 语句之前插入（两处）：
_auto_draft_replacement(app, candidate, decay_result)
```

在文件末尾追加辅助函数：
```python
def _auto_draft_replacement(app: Any, candidate: Any, decay_result: Dict[str, Any]) -> None:
    """Auto-create a draft replacement proposal after decay demotion (best-effort)."""
    try:
        from core.research.orchestrator import create_manual_proposal
        symbol = (candidate.symbols or ["BTC/USDT"])[0]
        thesis = (
            f"替代策略研究（自动生成）：{candidate.strategy} 在 {symbol} 上触发 CUSUM 衰减"
            f"（衰减 {decay_result.get('decay_pct', 0):.1f}%），寻找替代方向。"
        )
        new_proposal = create_manual_proposal(
            app,
            thesis=thesis,
            symbols=[symbol],
            timeframes=getattr(candidate, "timeframes", ["15m", "1h"]) or ["15m", "1h"],
            market_regime="mixed",
            source="cusum_auto",
            notes=[f"由 CUSUM 衰减自动生成，原候选: {candidate.candidate_id}"],
            metadata={"parent_candidate_id": candidate.candidate_id, "auto_generated": True},
        )
        logger.info(f"cusum_watcher: auto-drafted replacement proposal {new_proposal.proposal_id} for {candidate.candidate_id}")
    except Exception as exc:
        logger.debug(f"cusum_watcher: auto-draft failed (non-fatal): {exc}")
```

**注意**：`create_manual_proposal` 已存在于 `core/research/orchestrator.py`，直接复用，无需新增参数。

---

## Phase D — 订单预览桥（最小可行版，不自动下单）

**目标**：候选详情面板加"生成订单预览"按钮，展示 SignalAggregator 输出的建议订单（不执行）

### D1. 新增预览端点

**文件**：`web/api/ai_research.py`

```python
@router.post("/candidates/{candidate_id}/order-preview")
async def generate_order_preview(request: Request, candidate_id: str):
    """Generate a suggested order preview from SignalAggregator. Does NOT place any order.

    Returns: direction, estimated_size_usdt, stop_loss_pct, take_profit_pct, confidence,
             requires_approval, blocked_by_risk, component breakdown.
    """
    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    if str(cand.status) not in {"validated", "paper_running", "shadow_running", "live_candidate", "live_running"}:
        raise HTTPException(status_code=400, detail=f"候选状态 {cand.status} 不支持生成订单预览")

    cand_symbol = (cand.symbols or ["BTC/USDT"])[0]
    allocation_pct = float(cand.metadata.get("allocation_pct") or 0.05)

    # Lazy singleton aggregator
    if not hasattr(request.app.state, "_signal_aggregator"):
        from core.ai.signal_aggregator import SignalAggregator
        request.app.state._signal_aggregator = SignalAggregator()
    aggregator = request.app.state._signal_aggregator

    import pandas as pd
    df = pd.DataFrame()
    try:
        from core.strategies import strategy_manager as sm
        df = sm._load_market_data(cand_symbol, "1h")
    except Exception:
        pass

    sig = await aggregator.aggregate(cand_symbol, df)

    # Estimate capital from risk_manager (read-only)
    total_capital = 10000.0
    try:
        from core.risk.risk_manager import risk_manager
        total_capital = float(getattr(risk_manager, "_cached_equity", None) or 10000.0)
    except Exception:
        pass

    size_usdt = round(total_capital * allocation_pct, 2)

    # Stop/take from signal_engine risk calc (read from LLM component if available)
    llm_comp = sig.components.get("llm", {})
    stop_loss_pct = 0.03
    take_profit_pct = 0.06
    # Use ATR-based values if available from last bar
    if not df.empty and "atr" in df.columns:
        atr_pct = float(df["atr"].iloc[-1])
        if atr_pct > 0:
            stop_loss_pct = round(min(max(atr_pct * 1.4, 0.01), 0.15), 4)
            take_profit_pct = round(stop_loss_pct * 2.0, 4)

    return {
        "candidate_id": candidate_id,
        "symbol": cand_symbol,
        "direction": sig.direction,
        "confidence": sig.confidence,
        "requires_approval": sig.requires_approval,
        "blocked_by_risk": sig.blocked_by_risk,
        "risk_reason": sig.risk_reason,
        "size_usdt": size_usdt,
        "allocation_pct": allocation_pct,
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": take_profit_pct,
        "components": sig.components,
        "note": "此为预览，不会自动下单。确认后请在交易面板手动执行。",
        "ts": sig.timestamp.isoformat(),
    }
```

### D2. 候选详情面板添加预览按钮

**文件**：`web/static/js/ai_research.js`

找到 `viewCandidate(id)` 或 `buildCandidateCard()` 中候选详情面板的操作按钮区域，追加：

```javascript
// 在详情面板操作按钮区末尾追加（不替换任何现有按钮）
<button class="btn btn-sm btn-outline-primary" onclick="showOrderPreview('${cand.candidate_id}')">
  生成订单预览
</button>
```

在文件末尾追加：
```javascript
async function showOrderPreview(candidateId) {
    const btn = event.target;
    btn.disabled = true;
    btn.textContent = '计算中...';
    try {
        const r = await apiFetch(`/api/ai/candidates/${candidateId}/order-preview`, { method: 'POST' });
        const dirColor = r.direction === 'LONG' ? '#4ade80' : r.direction === 'SHORT' ? '#f87171' : '#94a3b8';
        const blockedHtml = r.blocked_by_risk
            ? `<div style="color:#f87171;margin-top:8px">⚠ 风控拦截：${r.risk_reason}</div>` : '';
        const approvalHtml = r.requires_approval && !r.blocked_by_risk
            ? `<div style="color:#fcd34d;margin-top:8px">⚠ 置信度不足（${(r.confidence*100).toFixed(0)}%），建议人工确认</div>` : '';
        const comp = r.components || {};
        const pct = v => (v*100).toFixed(1)+'%';
        const dirIcon = d => d==='LONG'?'▲':d==='SHORT'?'▼':'─';

        const html = `
<div style="font-size:13px;line-height:1.6">
  <div style="font-size:16px;font-weight:700;color:${dirColor};margin-bottom:12px">
    ${dirIcon(r.direction)} ${r.direction} &nbsp; 置信度 ${pct(r.confidence)}
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:12px">
    <tr><td style="color:var(--text-muted)">标的</td><td style="font-weight:600">${r.symbol}</td></tr>
    <tr><td style="color:var(--text-muted)">建议仓位</td><td>${r.size_usdt.toLocaleString()} USDT（${pct(r.allocation_pct)}）</td></tr>
    <tr><td style="color:var(--text-muted)">止损</td><td>${pct(r.stop_loss_pct)}</td></tr>
    <tr><td style="color:var(--text-muted)">止盈</td><td>${pct(r.take_profit_pct)}</td></tr>
  </table>
  <div style="margin-top:12px;font-size:11px;color:var(--text-muted)">信号分解</div>
  <div style="display:flex;gap:8px;margin-top:4px">
    ${['llm','ml','factor'].map(k=>{const c=comp[k]||{};return`
    <div style="flex:1;background:var(--bg-3);border-radius:6px;padding:6px 8px;font-size:11px">
      <div style="font-weight:600;text-transform:uppercase">${k}</div>
      <div style="color:${dirColor};font-size:13px">${dirIcon(c.direction||'FLAT')} ${c.direction||'FLAT'}</div>
      <div style="color:var(--text-muted)">${pct(c.confidence||0)} × ${pct(c.weight||0)}</div>
    </div>`;}).join('')}
  </div>
  ${blockedHtml}${approvalHtml}
  <div style="margin-top:12px;font-size:11px;color:var(--text-muted);font-style:italic">${r.note}</div>
</div>`;

        // 显示在候选详情面板的 #ai-order-preview-result 区域
        const previewEl = document.getElementById('ai-order-preview-result');
        if (previewEl) {
            previewEl.innerHTML = html;
            previewEl.style.display = 'block';
        } else {
            // 降级：用 alert 样式的 modal
            showModal('订单预览', html);
        }
    } catch(e) {
        showToast(`预览失败: ${e.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = '生成订单预览';
    }
}
```

**文件**：`web/templates/index.html`

在候选详情面板（`#ai-detail-panel` 或详情 section 内）底部添加：
```html
<!-- 订单预览结果区 -->
<div id="ai-order-preview-result" style="display:none;margin-top:12px;padding:12px;background:var(--bg-2);border-radius:8px;border:1px solid var(--border)"></div>
```

---

## Phase E — UI/UX 改善

> 全部为前端改动 + 轻量 API，不触及核心引擎。

### E1. 候选卡片验证流水线进度条

**目标**：让用户在卡片上就能看到 IS/OOS/WF/DSR 哪步通过、哪步失败，不必点进详情。

**文件**：`web/static/js/ai_research.js`

在 `buildCandidateCard(cand)` 函数内，卡片 HTML 底部追加一行进度条 HTML（不替换现有逻辑）：

```javascript
// 追加到 buildCandidateCard 返回的 HTML 末尾，卡片 footer 之前
function _renderValidationPipeline(cand) {
    const vs = cand.validation_summary || {};
    // 6步流水线，每步取对应分数判断状态
    const steps = [
        { label: '数据',  pass: vs.data_readiness_score != null },
        { label: 'IS',    val: vs.is_score,    thresh: 0.3 },
        { label: 'OOS',   val: vs.oos_score,   thresh: 0.3 },
        { label: 'WF',    val: vs.wf_stability,thresh: 0.5 },
        { label: 'DSR',   val: vs.dsr_score,   thresh: 0.3 },
        { label: '风控',  pass: vs.risk_score != null && vs.risk_score > 0.2 },
    ];
    const dot = s => {
        if (s.pass === false) return '<span class="vp-dot vp-skip">─</span>';
        const v = s.val;
        if (v == null) return '<span class="vp-dot vp-na">?</span>';
        const cls = v >= s.thresh * 1.5 ? 'vp-ok' : v >= s.thresh ? 'vp-warn' : 'vp-fail';
        return `<span class="vp-dot ${cls}" title="${s.label}: ${(v*100).toFixed(0)}%">${s.label}</span>`;
    };
    return `<div class="vp-bar">${steps.map(dot).join('<span class="vp-arrow">›</span>')}</div>`;
}
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* E1 — validation pipeline bar */
.vp-bar { display:flex; align-items:center; gap:2px; margin-top:6px; flex-wrap:wrap; }
.vp-dot { font-size:10px; padding:1px 5px; border-radius:4px; font-weight:600; cursor:default; }
.vp-ok   { background:#14532d; color:#86efac; }
.vp-warn { background:#78350f; color:#fcd34d; }
.vp-fail { background:#7f1d1d; color:#fca5a5; }
.vp-na   { background:var(--bg-3); color:var(--text-muted); }
.vp-skip { background:transparent; color:var(--text-muted); }
.vp-arrow { color:var(--text-muted); font-size:10px; }
```

---

### E2. 候选生命周期状态机步进条

**目标**：详情面板顶部展示候选当前处于哪个生命周期阶段，已完成打勾，未来阶段置灰。

**文件**：`web/static/js/ai_research.js`

新增纯前端辅助函数（不调用任何新 API），在详情面板渲染逻辑中调用：

```javascript
// 生命周期步进条，纯 HTML/CSS，不需要新 API
const LIFECYCLE_STEPS = [
    { key: 'draft',          label: '起草' },
    { key: 'research_queued',label: '排队' },
    { key: 'research_running',label: '研究' },
    { key: 'validated',      label: '验证' },
    { key: 'paper_running',  label: '纸盘' },
    { key: 'shadow_running', label: '影子' },
    { key: 'live_candidate', label: '候选' },
    { key: 'live_running',   label: '实盘' },
];

function renderLifecycleStepper(currentStatus) {
    const rejected = currentStatus === 'rejected';
    const retired  = currentStatus === 'retired';
    const curIdx   = LIFECYCLE_STEPS.findIndex(s => s.key === currentStatus);

    return `<div class="lc-stepper">
        ${LIFECYCLE_STEPS.map((s, i) => {
            let cls = 'lc-step';
            if (rejected || retired) cls += ' lc-inactive';
            else if (i < curIdx)  cls += ' lc-done';
            else if (i === curIdx) cls += ' lc-active';
            else cls += ' lc-future';
            const check = (!rejected && !retired && i < curIdx) ? '✓ ' : '';
            return `<div class="${cls}"><span>${check}${s.label}</span></div>
                    ${i < LIFECYCLE_STEPS.length-1 ? '<div class="lc-connector"></div>' : ''}`;
        }).join('')}
        ${rejected ? '<div class="lc-step lc-rejected">已拒绝</div>' : ''}
        ${retired  ? '<div class="lc-step lc-rejected">已退役</div>' : ''}
    </div>`;
}
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* E2 — lifecycle stepper */
.lc-stepper { display:flex; align-items:center; flex-wrap:wrap; gap:0; margin-bottom:14px; }
.lc-step { font-size:11px; padding:3px 8px; border-radius:12px; white-space:nowrap; }
.lc-connector { width:16px; height:1px; background:var(--border); flex-shrink:0; }
.lc-done   { background:#14532d; color:#86efac; }
.lc-active { background:#1e40af; color:#93c5fd; font-weight:700; }
.lc-future { background:var(--bg-3); color:var(--text-muted); }
.lc-inactive { background:var(--bg-3); color:var(--text-muted); opacity:0.5; }
.lc-rejected { background:#7f1d1d; color:#fca5a5; font-weight:700; }
```

---

### E3. 审批队列卡片信息密度提升

**目标**：审批卡片直接展示核心指标 + 微型权益曲线，5秒内完成决策。

**文件**：`web/static/js/ai_research.js`

修改 `renderApprovalQueue()` 中每张卡片的 HTML 模板，在现有 approve/reject 按钮上方插入信息区：

```javascript
// 在审批卡片内插入，不改动 humanApprove/humanReject 按钮
function _approvalCardMeta(c) {
    const vs = c.validation_summary || {};
    const sp = v => v != null ? v.toFixed(2) : 'N/A';
    const pct = v => v != null ? (v*100).toFixed(1)+'%' : 'N/A';

    // 微型 sparkline（equity_curve_sample 50点，已存在于 metadata）
    const eq = (c.metadata || {}).equity_curve_sample || [];
    const sparkline = eq.length > 1 ? _inlineSparkline(eq, 120, 28) : '';

    return `
<div class="appr-meta">
  <div class="appr-metrics">
    <span class="appr-m"><span class="appr-ml">Sharpe</span><b>${sp(vs.edge_score)}</b></span>
    <span class="appr-m"><span class="appr-ml">OOS</span><b>${sp(vs.oos_score)}</b></span>
    <span class="appr-m"><span class="appr-ml">DSR</span><b>${pct(vs.dsr_score)}</b></span>
    <span class="appr-m"><span class="appr-ml">WF稳定</span><b>${pct(vs.wf_stability)}</b></span>
  </div>
  ${sparkline ? `<div class="appr-spark">${sparkline}</div>` : ''}
</div>`;
}

// 内联 SVG sparkline（纯前端，无依赖）
function _inlineSparkline(points, w, h) {
    const mn = Math.min(...points), mx = Math.max(...points);
    const range = mx - mn || 1;
    const xs = points.map((_, i) => (i / (points.length - 1)) * w);
    const ys = points.map(v => h - ((v - mn) / range) * h);
    const d = xs.map((x, i) => `${i===0?'M':'L'}${x.toFixed(1)},${ys[i].toFixed(1)}`).join(' ');
    const color = points[points.length-1] >= points[0] ? '#4ade80' : '#f87171';
    return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
        <polyline points="${xs.map((x,i)=>x.toFixed(1)+','+ys[i].toFixed(1)).join(' ')}"
            fill="none" stroke="${color}" stroke-width="1.5" stroke-linejoin="round"/>
    </svg>`;
}
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* E3 — approval card meta */
.appr-meta { margin-bottom:8px; }
.appr-metrics { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:6px; }
.appr-m { font-size:12px; display:flex; flex-direction:column; align-items:flex-start; }
.appr-ml { font-size:10px; color:var(--text-muted); }
.appr-m b { font-size:13px; }
.appr-spark { line-height:0; }
```

---

### E4. 参数敏感性可视化（单参数扰动）

**目标**：展示各参数对 Sharpe 的影响幅度，帮助判断最优参数是否稳健。

**文件**：`web/api/ai_research.py`（末尾追加）

```python
@router.get("/candidates/{candidate_id}/param-sensitivity")
async def get_param_sensitivity(request: Request, candidate_id: str):
    """Compute single-parameter sensitivity: perturb each best_param ±20%, report Sharpe delta.

    Uses a simplified 1-strategy backtest (no optimization loop). Max runtime ~5s.
    Returns list of {param, base_val, low_val, high_val, sharpe_base, sharpe_low, sharpe_high}.
    """
    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    vs = getattr(cand, "validation_summary", None)
    best_params = {}
    if vs:
        best_params = (vs.best_params if hasattr(vs, "best_params") else {}) or {}
    if not best_params:
        best_params = cand.metadata.get("best_params", {})

    if not best_params:
        return {"items": [], "note": "候选策略无 best_params，无法计算敏感性"}

    symbol = (cand.symbols or ["BTC/USDT"])[0]
    timeframe = (cand.timeframes or ["1h"])[0]

    # Lazy import backtest runner (already exists)
    from web.api.backtest import _run_backtest_core, BacktestConfig  # noqa: PLC0415

    results = []
    for param, base_val in best_params.items():
        try:
            base_v = float(base_val)
        except (TypeError, ValueError):
            continue
        if base_v == 0:
            continue

        row = {"param": param, "base_val": base_v}
        for label, mult in [("low", 0.80), ("base", 1.0), ("high", 1.20)]:
            perturbed = {**best_params, param: base_v * mult}
            try:
                cfg = BacktestConfig(
                    strategy=cand.strategy,
                    symbol=symbol,
                    timeframe=timeframe,
                    days=30,
                    params=perturbed,
                )
                res = await _run_backtest_core(cfg)
                row[f"sharpe_{label}"] = round(float(res.get("sharpe_ratio") or 0), 3)
                row[f"{label}_val"] = round(base_v * mult, 6)
            except Exception:
                row[f"sharpe_{label}"] = None
                row[f"{label}_val"] = round(base_v * mult, 6)
        results.append(row)

    return {"candidate_id": candidate_id, "items": results, "base_params": best_params}
```

**文件**：`web/static/js/ai_research.js`（末尾追加）：

```javascript
// E4 — 参数敏感性图，在候选详情面板加"参数敏感性"折叠区
async function loadParamSensitivity(candidateId) {
    const el = document.getElementById('ai-param-sensitivity');
    if (!el) return;
    el.innerHTML = '<span class="text-muted" style="font-size:12px">计算中...</span>';
    try {
        const r = await apiFetch(`/api/ai/candidates/${candidateId}/param-sensitivity`);
        if (!r.items || !r.items.length) {
            el.innerHTML = `<span class="text-muted" style="font-size:12px">${r.note || '无数据'}</span>`;
            return;
        }
        // 水平条形图：low/base/high 三段，颜色编码
        el.innerHTML = r.items.map(item => {
            const vals = [item.sharpe_low, item.sharpe_base, item.sharpe_high].map(v => v ?? 0);
            const mx = Math.max(...vals.map(Math.abs)) || 1;
            const bar = (v, color) => {
                const w = Math.round(Math.abs(v) / mx * 80);
                const sign = v >= 0 ? '' : '-';
                return `<span style="display:inline-block;width:${w}px;height:8px;background:${color};border-radius:2px;margin-right:2px;vertical-align:middle"></span><span style="font-size:10px">${sign}${Math.abs(v).toFixed(2)}</span>`;
            };
            return `
<div class="ps-row">
  <span class="ps-param">${item.param}</span>
  <div class="ps-bars">
    <div class="ps-bar-row">${bar(item.sharpe_low,'#f87171')} <span class="ps-lbl">-20%</span></div>
    <div class="ps-bar-row">${bar(item.sharpe_base,'#60a5fa')} <span class="ps-lbl">基准</span></div>
    <div class="ps-bar-row">${bar(item.sharpe_high,'#4ade80')} <span class="ps-lbl">+20%</span></div>
  </div>
</div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<span class="text-muted" style="font-size:12px">加载失败</span>`;
    }
}
```

在详情面板 HTML (`index.html`) 底部加容器（紧接 `#ai-order-preview-result` 之后）：
```html
<details style="margin-top:10px">
  <summary style="font-size:12px;cursor:pointer;color:var(--text-muted)">参数敏感性分析</summary>
  <div id="ai-param-sensitivity" style="margin-top:8px;padding:8px;background:var(--bg-2);border-radius:6px"></div>
</details>
```

在详情面板打开时（`viewCandidate` 函数中）追加一行：
```javascript
loadParamSensitivity(cand.candidate_id);
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* E4 — param sensitivity */
.ps-row { display:flex; align-items:flex-start; gap:8px; margin-bottom:8px; }
.ps-param { font-size:11px; color:var(--text-muted); width:90px; flex-shrink:0; padding-top:2px; word-break:break-all; }
.ps-bars { flex:1; }
.ps-bar-row { display:flex; align-items:center; gap:4px; margin-bottom:2px; }
.ps-lbl { font-size:10px; color:var(--text-muted); width:24px; }
```

---

### E5. 多候选并排对比

**目标**：在 AI Hub 卡片列表中，最多勾选3个候选，点"对比"按钮弹出并排指标表格。

**完全纯前端，不需要新 API**（复用已有 `state.candidateList` 数据）。

**文件**：`web/static/js/ai_research.js`（末尾追加）：

```javascript
// E5 — 多候选对比
let _compareSelected = new Set();

function toggleCompare(candidateId) {
    if (_compareSelected.has(candidateId)) {
        _compareSelected.delete(candidateId);
    } else {
        if (_compareSelected.size >= 3) { showToast('最多同时对比3个候选', 'warn'); return; }
        _compareSelected.add(candidateId);
    }
    // 更新所有卡片的勾选状态
    document.querySelectorAll('.cand-compare-cb').forEach(cb => {
        cb.checked = _compareSelected.has(cb.dataset.cid);
    });
    document.getElementById('ai-compare-btn').style.display =
        _compareSelected.size >= 2 ? 'inline-block' : 'none';
}

function showComparePanel() {
    if (_compareSelected.size < 2) return;
    const cands = [..._compareSelected].map(id =>
        (state.candidateList || []).find(c => c.candidate_id === id)
    ).filter(Boolean);

    const METRICS = [
        ['策略', c => c.strategy],
        ['状态', c => c.status],
        ['Sharpe(IS)', c => (c.validation_summary?.is_score ?? 'N/A')],
        ['Sharpe(OOS)', c => (c.validation_summary?.oos_score ?? 'N/A')],
        ['WF稳定性', c => c.validation_summary?.wf_stability != null ? (c.validation_summary.wf_stability*100).toFixed(0)+'%' : 'N/A'],
        ['DSR', c => c.validation_summary?.dsr_score != null ? (c.validation_summary.dsr_score*100).toFixed(0)+'%' : 'N/A'],
        ['风险评分', c => (c.validation_summary?.risk_score ?? 'N/A')],
        ['标的', c => (c.symbols||[]).join(', ')],
        ['时间框架', c => (c.timeframes||[]).join(', ')],
        ['分配比例', c => c.metadata?.allocation_pct != null ? (c.metadata.allocation_pct*100).toFixed(0)+'%' : '未设置'],
    ];

    const thead = `<tr><th>指标</th>${cands.map(c=>`<th>${c.strategy}<br><span style="font-size:10px;color:var(--text-muted)">${c.candidate_id.slice(0,8)}</span></th>`).join('')}</tr>`;
    const tbody = METRICS.map(([label, fn]) =>
        `<tr><td style="color:var(--text-muted);font-size:12px">${label}</td>${cands.map(c=>`<td style="font-size:12px;font-weight:500">${fn(c)}</td>`).join('')}</tr>`
    ).join('');

    showModal('候选策略对比', `<table class="compare-table"><thead>${thead}</thead><tbody>${tbody}</tbody></table>`);
}
```

在候选卡片 HTML 末尾（`buildCandidateCard` 内）追加：
```javascript
// 在卡片右上角追加勾选框
<input type="checkbox" class="cand-compare-cb" data-cid="${cand.candidate_id}"
    onchange="toggleCompare('${cand.candidate_id}')" style="position:absolute;top:8px;right:8px">
```

在 AI Hub 标题区（`index.html`）追加"对比"按钮：
```html
<button id="ai-compare-btn" class="btn btn-sm btn-outline-warning"
    onclick="showComparePanel()" style="display:none">对比选中</button>
```

**文件**：`web/static/css/style.css`（末尾追加）：
```css
/* E5 — compare table */
.compare-table { width:100%; border-collapse:collapse; font-size:12px; }
.compare-table th { padding:6px 10px; border-bottom:1px solid var(--border); font-weight:600; text-align:left; }
.compare-table td { padding:5px 10px; border-bottom:1px solid var(--border-light,var(--border)); }
.compare-table tr:hover td { background:var(--bg-2); }
```

---

## Phase F — 数据源扩充

> 分三档：免费无需 Key、免费需 Key、付费可选。

### 现有但未充分利用（优先接入，零新增依赖）

| 现有组件 | 位置 | 当前状态 | 接入目标 |
|---|---|---|---|
| `FearGreedCollector` | `core/data/sentiment/fear_greed_collector.py` | 独立运行，未接入信号流 | 接入 `SignalAggregator` factor 分量 |
| `OICollector` | `core/data/oi_collector.py` | 独立运行，未接入 planner | 接入 `research_planner._parse_market_context` |
| `OrderBookCollector` | `core/data/orderbook/orderbook_collector.py` | 实时快照，无时序存储 | OFI 时序聚合存 SQLite，供 planner 使用 |

#### F0a. 恐惧贪婪指数接入 SignalAggregator

**文件**：`core/ai/signal_aggregator.py`

在 `_get_factor_signal(market_data)` 方法末尾，追加一个 fear/greed 调整项（±0.05 权重修正）：

```python
# 在 _get_factor_signal 计算出 direction/conf 后，追加：
try:
    from core.data.sentiment.fear_greed_collector import fear_greed_collector  # noqa: PLC0415
    fg = fear_greed_collector._history[0] if fear_greed_collector._history else None
    if fg:
        # 极度恐惧 → 强化 LONG 信号；极度贪婪 → 强化 SHORT 信号
        if fg.is_extreme_fear and direction == "LONG":
            conf = min(1.0, conf + 0.08)
        elif fg.is_extreme_greed and direction == "SHORT":
            conf = min(1.0, conf + 0.08)
except Exception:
    pass
```

**注意**：`fear_greed_collector` 全局单例已在模块末尾定义，直接 import 使用。

#### F0b. OI 变化率接入 research_planner

**文件**：`core/ai/research_planner.py`

在 `_parse_market_context(context)` 方法中，在现有 whale/funding 处理之后追加：

```python
# OI 变化率
oi_change_pct = float((context or {}).get("oi_change_pct") or 0.0)
if oi_change_pct > 10:   # OI 急升 → 加杠杆，趋势偏强
    boosts.setdefault("趋势", 0)
    boosts["趋势"] += 0.15
    notes.append(f"OI急升{oi_change_pct:.1f}%→boost趋势")
elif oi_change_pct < -10:  # OI 急降 → 去杠杆，震荡偏强
    boosts.setdefault("震荡", 0)
    boosts["震荡"] += 0.10
    notes.append(f"OI急降{abs(oi_change_pct):.1f}%→boost震荡")
```

在前端 `_collectLiveMarketContext(symbol)` 中（`ai_research.js` 已有），追加 OI fetch：
```javascript
// 在 _collectLiveMarketContext 的 Promise.allSettled 数组里追加：
apiFetch(`/api/trading/market_microstructure?symbol=${encodeURIComponent(symbol)}&exchange=binance`)
    .then(r => { ctx.oi_change_pct = r?.oi?.change_pct_1h ?? 0; })
    .catch(() => {}),
```

---

### F1. Deribit 期权数据（免费公开 API，无需 Key）

**新建文件**：`core/data/options_collector.py`（约 120 行）

```python
"""Deribit public options market data collector.

Fetches 25-delta IV skew, ATM implied volatility, and Put/Call OI ratio.
No authentication required — uses Deribit public REST API.

Usage:
    collector = DeribitOptionsCollector()
    snap = await collector.fetch_snapshot("BTC")
    # snap.atm_iv, snap.skew_25d, snap.put_call_ratio
"""
from __future__ import annotations
import asyncio
import aiohttp
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from loguru import logger


@dataclass
class OptionsSnapshot:
    currency: str          # "BTC" | "ETH"
    atm_iv: float          # ATM implied vol (annualized %)
    skew_25d: float        # 25-delta skew = (put_iv - call_iv) / atm_iv; >0 = fear premium
    put_call_ratio: float  # put OI / call OI; >1 = bearish hedging
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "currency": self.currency,
            "atm_iv": round(self.atm_iv, 4),
            "skew_25d": round(self.skew_25d, 4),
            "put_call_ratio": round(self.put_call_ratio, 4),
            "timestamp": self.timestamp.isoformat(),
            # Derived signals
            "signal": "fear" if self.skew_25d > 0.05 or self.put_call_ratio > 1.3
                      else "greed" if self.skew_25d < -0.05 and self.put_call_ratio < 0.7
                      else "neutral",
        }


class DeribitOptionsCollector:
    _BASE = "https://www.deribit.com/api/v2/public"

    def __init__(self, timeout: int = 10):
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def fetch_snapshot(self, currency: str = "BTC") -> Optional[OptionsSnapshot]:
        """Fetch current options market snapshot for given currency."""
        url = f"{self._BASE}/get_book_summary_by_currency"
        params = {"currency": currency.upper(), "kind": "option"}
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(f"Deribit API {resp.status} for {currency}")
                        return None
                    data = await resp.json()
                    instruments = data.get("result") or []
                    return self._parse_snapshot(currency, instruments)
        except Exception as exc:
            logger.debug(f"DeribitOptionsCollector: {exc}")
            return None

    def _parse_snapshot(self, currency: str, instruments: list) -> Optional[OptionsSnapshot]:
        if not instruments:
            return None
        call_oi = put_oi = 0.0
        call_ivs = []
        put_ivs = []
        for inst in instruments:
            name = str(inst.get("instrument_name") or "")
            iv = inst.get("mark_iv") or 0.0
            oi = inst.get("open_interest") or 0.0
            if name.endswith("-C"):
                call_oi += oi
                if iv > 0: call_ivs.append(iv)
            elif name.endswith("-P"):
                put_oi += oi
                if iv > 0: put_ivs.append(iv)

        atm_iv = ((sum(call_ivs) / len(call_ivs)) if call_ivs else 0.0) / 100.0
        avg_put_iv = (sum(put_ivs) / len(put_ivs) if put_ivs else 0.0) / 100.0
        avg_call_iv = atm_iv
        skew = ((avg_put_iv - avg_call_iv) / atm_iv) if atm_iv > 0 else 0.0
        pc_ratio = (put_oi / call_oi) if call_oi > 0 else 1.0

        return OptionsSnapshot(
            currency=currency.upper(),
            atm_iv=round(atm_iv, 4),
            skew_25d=round(skew, 4),
            put_call_ratio=round(pc_ratio, 4),
        )


options_collector = DeribitOptionsCollector()
```

**接入点**（最小改动）：

1. **`web/api/trading.py`** `get_market_microstructure()` 末尾追加期权快照字段（已有 funding/basis 追加模式）：
```python
# 追加期权数据（best-effort）
options_snap = {}
try:
    from core.data.options_collector import options_collector  # noqa: PLC0415
    cur = symbol.split("/")[0].split(":")[0]
    snap = await options_collector.fetch_snapshot(cur)
    if snap:
        options_snap = snap.to_dict()
except Exception:
    pass
# 在 return dict 里加一个字段：
# "options": options_snap,
```

2. **`core/ai/research_planner.py`** `_parse_market_context` 末尾追加：
```python
# 期权偏斜信号
options_skew = float((context or {}).get("options_skew_25d") or 0.0)
if options_skew > 0.08:   # 明显 put 溢价 → 市场在对冲下行风险
    boosts.setdefault("风险", 0)
    boosts["风险"] += 0.12
    notes.append(f"期权偏斜{options_skew:.3f}→boost风险")
elif options_skew < -0.05: # call 溢价 → FOMO 情绪
    boosts.setdefault("趋势", 0)
    boosts["趋势"] += 0.08
    notes.append(f"期权call溢价{abs(options_skew):.3f}→boost趋势")
```

---

### F2. Google Trends（免费，需安装 `pytrends`）

**新建文件**：`core/data/google_trends_collector.py`（约 80 行）

```python
"""Google Trends collector via pytrends (unofficial API).

pip install pytrends

Collects 7-day hourly interest data for crypto keywords.
Saves to data/google_trends/<keyword>_trends.parquet.
Respects rate limits: max 1 request per 60s per keyword.
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
from loguru import logger


_CACHE_DIR = Path("data/google_trends")
_KEYWORDS = ["bitcoin", "crypto", "ethereum", "cryptocurrency crash", "buy bitcoin"]
_RATE_LIMIT_SEC = 65  # Google 429s are common below 60s


async def fetch_trends_async(keyword: str, timeframe: str = "now 7-d") -> Optional[pd.DataFrame]:
    """Run pytrends in a thread pool to avoid blocking the event loop."""
    def _sync_fetch():
        try:
            from pytrends.request import TrendReq  # noqa: PLC0415
            pt = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
            pt.build_payload([keyword], cat=0, timeframe=timeframe, geo="", gprop="")
            df = pt.interest_over_time()
            if df.empty:
                return None
            df = df[[keyword]].rename(columns={keyword: "interest"})
            df.index = pd.to_datetime(df.index, utc=True)
            return df
        except ImportError:
            logger.debug("pytrends not installed — Google Trends disabled")
            return None
        except Exception as exc:
            logger.debug(f"Google Trends fetch failed for {keyword!r}: {exc}")
            return None

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_fetch)


async def update_all_keywords(keywords: list = _KEYWORDS) -> Dict[str, int]:
    """Update local parquet caches for all keywords. Called from background task."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for kw in keywords:
        df = await fetch_trends_async(kw)
        if df is not None and not df.empty:
            path = _CACHE_DIR / f"{kw.replace(' ', '_')}_trends.parquet"
            df.to_parquet(path)
            results[kw] = len(df)
            logger.debug(f"Google Trends: saved {len(df)} rows for {kw!r}")
        await asyncio.sleep(_RATE_LIMIT_SEC)  # respect rate limit between keywords
    return results


def load_latest(keyword: str = "bitcoin") -> Optional[float]:
    """Load latest interest value (0-100) from local cache. Returns None if unavailable."""
    path = _CACHE_DIR / f"{keyword.replace(' ', '_')}_trends.parquet"
    try:
        df = pd.read_parquet(path)
        return float(df["interest"].iloc[-1])
    except Exception:
        return None
```

**接入 `web/main.py`**（在现有 lifespan background task 注册区末尾追加，不改动其他任务）：

```python
# Google Trends 每6小时更新（非关键，失败不影响系统）
async def _google_trends_worker():
    await asyncio.sleep(120)   # 启动后2分钟延迟（避免与其他任务抢资源）
    while True:
        try:
            from core.data.google_trends_collector import update_all_keywords  # noqa: PLC0415
            await update_all_keywords()
        except Exception as exc:
            logger.debug(f"google_trends_worker: {exc}")
        await asyncio.sleep(6 * 3600)

asyncio.create_task(_google_trends_worker())
```

**接入 research_planner**（`_parse_market_context` 末尾追加）：
```python
# Google Trends "bitcoin" 热度
try:
    from core.data.google_trends_collector import load_latest  # noqa: PLC0415
    trend_val = load_latest("bitcoin")
    if trend_val is not None:
        if trend_val > 75:  # 搜索热度高 → 零售 FOMO，拥挤风险
            boosts.setdefault("风险", 0)
            boosts["风险"] += 0.10
            notes.append(f"谷歌趋势bitcoin热度{trend_val:.0f}→boost风险/拥挤")
        elif trend_val < 20:  # 热度低 → 冷场，均值回归机会
            boosts.setdefault("均值回归", 0)
            boosts["均值回归"] += 0.08
            notes.append(f"谷歌趋势热度低{trend_val:.0f}→boost均值回归")
except Exception:
    pass
```

---

### F3. FRED 宏观数据（免费，需申请 API Key）

**新建文件**：`core/data/macro_collector.py`（约 90 行）

```python
"""FRED (Federal Reserve Economic Data) macro indicator collector.

Free API key: https://fred.stlouisfed.org/docs/api/api_key.html
Set FRED_API_KEY environment variable.

Collects: DXY (USD index), VIX, Fed Funds Rate, CPI YoY.
Saves to data/macro/<series>.parquet, updates daily.
"""
from __future__ import annotations
import os
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import aiohttp
from loguru import logger


_CACHE_DIR = Path("data/macro")
_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs
SERIES = {
    "dxy":      "DTWEXBGS",   # USD Trade-Weighted Index (Broad)
    "vix":      "VIXCLS",     # CBOE VIX
    "fed_rate": "FEDFUNDS",   # Federal Funds Rate
    "cpi_yoy":  "CPIAUCSL",   # CPI (compute YoY in code)
}


async def fetch_series(series_id: str, api_key: str, days: int = 365) -> Optional[pd.Series]:
    start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id, "api_key": api_key,
        "file_type": "json", "observation_start": start,
        "sort_order": "asc",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(_BASE_URL, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                obs = data.get("observations") or []
                records = {}
                for o in obs:
                    try:
                        records[o["date"]] = float(o["value"])
                    except (ValueError, KeyError):
                        pass
                if not records:
                    return None
                return pd.Series(records, name=series_id)
    except Exception as exc:
        logger.debug(f"FRED fetch {series_id}: {exc}")
        return None


async def update_macro_cache() -> Dict[str, int]:
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        logger.debug("FRED_API_KEY not set — macro data collection skipped")
        return {}
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, series_id in SERIES.items():
        s = await fetch_series(series_id, api_key)
        if s is not None and not s.empty:
            path = _CACHE_DIR / f"{name}.parquet"
            s.to_frame().to_parquet(path)
            results[name] = len(s)
    return results


def load_macro_snapshot() -> Dict[str, Optional[float]]:
    """Load latest values from local cache. Returns None for missing series."""
    out: Dict[str, Optional[float]] = {}
    for name in SERIES:
        try:
            df = pd.read_parquet(_CACHE_DIR / f"{name}.parquet")
            out[name] = float(df.iloc[-1, 0])
        except Exception:
            out[name] = None
    return out
```

**接入 `web/main.py`**（在 `_google_trends_worker` 后追加）：
```python
async def _macro_cache_worker():
    await asyncio.sleep(180)
    while True:
        try:
            from core.data.macro_collector import update_macro_cache  # noqa: PLC0415
            await update_macro_cache()
        except Exception as exc:
            logger.debug(f"macro_cache_worker: {exc}")
        await asyncio.sleep(24 * 3600)  # 每日更新

asyncio.create_task(_macro_cache_worker())
```

**接入 `research_planner`**（`_parse_market_context` 末尾追加）：
```python
# FRED 宏观
try:
    from core.data.macro_collector import load_macro_snapshot  # noqa: PLC0415
    macro = load_macro_snapshot()
    vix = macro.get("vix")
    if vix and vix > 30:    # 高 VIX → 风险环境
        boosts.setdefault("风险", 0); boosts["风险"] += 0.12
        notes.append(f"VIX={vix:.1f}→boost风险")
    elif vix and vix < 15:  # 低 VIX → 趋势环境
        boosts.setdefault("趋势", 0); boosts["趋势"] += 0.08
        notes.append(f"VIX={vix:.1f}→boost趋势")
except Exception:
    pass
```

---

### F4. 付费数据源（选配，有 Key 才启用）

以下数据源价值高但需付费 API，建议以**可选插件**形式集成：接口统一为 `available: bool` 字段，无 Key 时直接返回 `available: False`，不影响其他逻辑。

| 数据源 | 关键指标 | 接入文件 | 优先级 |
|---|---|---|---|
| **Glassnode** | SOPR、MVRV、NVT、交易所净流量 | `core/data/glassnode_collector.py` | 高 |
| **CryptoQuant** | 矿工储备变化、交易所流入/流出 | `core/data/cryptoquant_collector.py` | 高 |
| **Nansen** | 聪明钱地址流向、DEX LP 变动 | `core/data/nansen_collector.py` | 中 |
| **Kaiko** | 高精度 L2 tick 数据、跨所价差 | `core/data/kaiko_collector.py` | 中 |

每个文件结构遵循已有模式（参考 `oi_collector.py`）：
- `fetch_xxx(symbol, api_key)` → `Optional[dict]`
- `api_key = os.environ.get("XXX_API_KEY", "")`
- 无 Key 时 `return {"available": False, "reason": "API key not set"}`

---

## 验收测试

### Phase A
1. 启动服务：`python -m uvicorn web.main:app --host 0.0.0.0 --port 8000`
2. 将至少一个候选设为 `paper_running` 状态（或用已有的）
3. 访问 `http://localhost:8000/api/ai/candidates/live-signals`，确认返回 JSON 包含 `signal.direction`/`signal.components`
4. 打开 AI 研究 Hub 页面，30s 内右侧面板出现实时信号卡片

### Phase B
1. 打开 AI 研究 Hub → 审批队列
2. 点击"一键纸盘 5%"按钮
3. 确认弹窗，候选状态变为 `paper_running`，`metadata.allocation_pct = 0.05`
4. 接口：`curl -X POST http://localhost:8000/api/ai/candidates/{id}/quick-register -d '{"mode":"paper","allocation_pct":0.05}'`

### Phase C
1. 手动调用 CUSUM watcher（或等待5分钟自动触发）
2. 确认降级候选后，`list_proposals()` 新增一个 `source=cusum_auto` 的 `draft` 提案
3. 该提案的 `notes[0]` 包含原候选 ID

### Phase D
1. 打开候选详情面板（选中一个 `validated` 或 `paper_running` 候选）
2. 点击"生成订单预览"
3. 展示方向/仓位/止损止盈/三路信号分解
4. 确认**不**触发任何真实下单（检查 `execution_engine` 日志无新订单）

### Phase E
1. **E1**：候选卡片底部出现 `数据 › IS › OOS › WF › DSR › 风控` 彩色进度条
2. **E2**：详情面板顶部出现生命周期步进条，当前状态高亮蓝色
3. **E3**：审批队列卡片直接显示 Sharpe/OOS/DSR/WF 4项指标 + 微型权益曲线
4. **E4**：详情面板底部折叠区展开后显示参数 -20%/基准/+20% 三段条形图
5. **E5**：候选卡片出现勾选框；勾选≥2个后"对比选中"按钮出现；点击弹出对比表格

### Phase F
1. **F0a**：`http://localhost:8000/api/ai/candidates/live-signals` 中 `components.factor.confidence` 受 Fear&Greed 调整（极度恐惧时 LONG 置信度 +0.08）
2. **F1**：`http://localhost:8000/api/trading/market_microstructure?symbol=BTC/USDT` 返回 `options` 字段包含 `atm_iv`/`skew_25d`/`put_call_ratio`
3. **F2**：`data/google_trends/bitcoin_trends.parquet` 在服务启动约2分钟后生成（需安装 pytrends）
4. **F3**：设置 `FRED_API_KEY` 环境变量后，`data/macro/vix.parquet` 在启动约3分钟后生成

---

## 文件改动汇总

### A-D（一条龙流水线）

| 文件 | 改动类型 | 改动量 |
|---|---|---|
| `web/api/ai_research.py` | 末尾追加4个端点函数 | +~120行，不改现有函数 |
| `core/monitoring/cusum_watcher.py` | `_demote_on_decay` 各分支+1行，文件末尾+1函数 | +~20行 |
| `web/static/js/ai_research.js` | 追加5个新函数，在已有函数内各+1行调用 | +~150行 |
| `web/templates/index.html` | 新增3处 HTML 片段（各<5行） | +~12行 |
| `web/static/css/style.css` | 末尾追加新样式块 | +~25行 |

### E（UI/UX 改善）

| 文件 | 改动类型 | 改动量 |
|---|---|---|
| `web/static/js/ai_research.js` | 追加5组新函数（E1~E5），各在已有渲染函数内+1行调用 | +~200行 |
| `web/static/css/style.css` | 末尾追加5块新样式 | +~60行 |
| `web/api/ai_research.py` | 追加1个端点（E4 参数敏感性） | +~40行 |
| `web/templates/index.html` | 新增3处 HTML 片段 | +~10行 |

### F（数据源扩充）

| 文件 | 改动类型 | 改动量 |
|---|---|---|
| `core/data/options_collector.py` | **新建** | ~120行 |
| `core/data/google_trends_collector.py` | **新建** | ~80行 |
| `core/data/macro_collector.py` | **新建** | ~90行 |
| `core/ai/signal_aggregator.py` | `_get_factor_signal` 末尾追加 Fear&Greed 调整 | +~10行 |
| `core/ai/research_planner.py` | `_parse_market_context` 末尾追加3块 boost 逻辑 | +~30行 |
| `web/api/trading.py` | `get_market_microstructure` 末尾追加 options 字段 | +~10行 |
| `web/main.py` | lifespan 末尾追加2个 background task | +~15行 |

**不改动**：`core/backtest/`、`core/strategies/`、`core/risk/`、`core/exchanges/`、`config/database.py`、所有策略文件

---

## 依赖说明

| Phase | 新增依赖 | 安装命令 | 是否必须 |
|---|---|---|---|
| F2 | pytrends | `pip install pytrends` | 否（无则跳过） |
| F3 | 无（stdlib + aiohttp 已有） | — | 否（需 FRED_API_KEY） |
| F1 | 无（aiohttp 已有） | — | 否（Deribit 公开 API） |
| F4 | 各自 SDK | 各厂商文档 | 否（需付费 Key） |

---

## 注意事项

1. **`sm._load_market_data` 是私有方法**：已有30s TTL缓存，Phase A/D 直接复用，如果未来该方法被重命名，改为公共接口。
2. **ML模型可能未训练**：`SignalAggregator` 在 ML 模型不存在时自动降权为0，不影响 LLM+Factor 路径。
3. **`create_manual_proposal` 签名**：确认 `orchestrator.py` 中该函数接受 `source`、`notes`、`metadata` 参数（当前版本已支持）。
4. **并发**：`app.state._signal_aggregator` 单例在多线程环境下首次创建可能有竞态，但 FastAPI 的 async 环境是单事件循环，不会有问题。
5. **前端 `apiFetch`/`showToast`/`showModal`**：确认这些工具函数在 `ai_research.js` 中已存在；若无 `showModal`，改用 `alert()` 降级。
6. **Google Trends 速率限制**：`pytrends` 非官方库，每个关键词请求间隔 65s，全量更新约需 6 分钟。建议仅收集 2-3 个核心关键词。
7. **E4 参数敏感性性能**：每个参数运行3次 30天回测，若参数超过5个，总耗时约 15s。建议加 `max_params=5` 截断，或改为后台 Job 异步计算。

---

## 执行优先级建议

```
立即（本周）：   A → B → E1 → E2 → E3
短期（2周内）：  C → D → E4 → E5 → F0a → F0b → F1
中期（1个月）：  F2 → F3
长期（按需）：   F4（付费数据源）
```

每个 Phase 独立可部署，不依赖前一个 Phase 完成。建议每个 Phase 单独提交后测试再合并。

---

*本文档由 Claude Code 生成于 2026-03-11，供 Claude Code 和 Codex 执行使用。*
