# 策略实时监控面板 — 实施方案

> 状态：待实施
> 创建：2026-03-13
> 目标：在策略页面新增一个面板，展示运行中策略的 K 线价格曲线、信号标记（位置+强度）、权益曲线、持仓信息，30 秒自动刷新。

---

## 背景与现状分析

### 已有、可直接复用的数据

| 数据 | 来源 | 说明 |
|---|---|---|
| 信号历史 | `strategy.get_recent_signals(200)` | 含 timestamp、price、signal_type、strength，内存中最多 1000 条 |
| 交易历史 | `risk_manager.get_trade_history(limit=5000)` | 含 timestamp、pnl、strategy 字段，用于重建权益曲线 |
| OHLCV | `data_storage.load_klines_from_parquet()` | 本地 parquet 缓存，已用于 live-vs-backtest 端点 |
| Plotly.js | `index.html:9` | `plotly-2.35.2.min.js` 已加载 |
| 时间轴辅助 | `app.js:58` `plotlyTimeAxis()` | 已存在，直接调用 |
| 策略 info | `GET /api/strategies/{name}` | 含 symbol、timeframe、state、allocation |
| 持仓 | `position_manager.get_positions_by_strategy(name)` | 当前开仓列表 |

### Shadow 策略的限制

`promotion_engine.py:259` 中 shadow promotion 的 `started: False` — 策略实例被创建但未注册到 `strategy_manager`，未启动，因此：
- 无信号历史
- 无交易记录
- 监控面板对 shadow 候选显示空数据

长期解法：修复 `promotion_engine.py:259`，让 shadow 以 paper 模式实际启动。短期：在 AI 研究候选详情面板使用已有的 `/order-preview` 端点展示实时信号。

### 面板位置

在策略 tab 现有「策略参数编辑」card（`#strategy-edit-panel`）下方新增一个 `wide` card。点击策略卡片时同步激活（与编辑面板联动，各自独立，不互相替换）。

---

## 改动清单

| 文件 | 类型 | 估计行数 |
|---|---|---|
| `web/api/strategies.py` | 末尾追加1个端点 + 1个辅助函数 | +~90行 |
| `web/templates/index.html` | 在策略 tab 内追加1个 card | +~18行 |
| `web/static/js/app.js` | 追加3个函数，`openEditor` 末尾 +1行调用 | +~130行 |
| `web/static/css/style.css` | 末尾追加样式块 | +~20行 |

---

## Phase 1 — 后端：新增监控数据端点

**文件：`web/api/strategies.py`**

在文件**末尾**（`get_strategy_signals` 函数之后）追加以下代码。

注意事项：
- 文件顶部已有 `from datetime import datetime, timezone`、`from core.strategies import strategy_manager`、`from core.risk.risk_manager import risk_manager`，不需要重复。
- `_timeframe_to_seconds` 如果文件中已有同名函数，删除下方定义直接复用。

```python
@router.get("/{name}/monitor-data")
async def get_strategy_monitor_data(name: str, bars: int = 200):
    """Return combined OHLCV + signals + equity curve for the strategy monitor panel.

    Data sources (all read-only, no side effects):
      - OHLCV: data_storage.load_klines_from_parquet (local parquet cache)
      - Signals: strategy.get_recent_signals(200) (in-memory, last 1000 max)
      - Equity curve: risk_manager.get_trade_history filtered by strategy name,
                      reconstructed with per-trade timestamps
      - Positions: position_manager.get_positions_by_strategy(name)
    """
    from datetime import timedelta
    from core.data.data_storage import data_storage
    from core.trading.position_manager import position_manager

    bars = max(50, min(int(bars), 500))

    # ── 1. Strategy info ─────────────────────────────────────────────────────
    strategy = strategy_manager.get_strategy(name)
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")

    symbol = (info.get("symbols") or ["BTC/USDT"])[0]
    timeframe = str(info.get("timeframe") or "1h")
    is_running = bool(info.get("state") == "running")

    # ── 2. OHLCV bars ────────────────────────────────────────────────────────
    ohlcv: list = []
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(
            seconds=_timeframe_to_seconds(timeframe) * (bars + 50)
        )
        df = await data_storage.load_klines_from_parquet(
            exchange=str(info.get("exchange") or "binance"),
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
        )
        if df is not None and not df.empty:
            df = df.tail(bars)
            for row in df.itertuples():
                ts = row.Index
                ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                ohlcv.append({
                    "t": ts_str,
                    "o": float(row.open)   if hasattr(row, "open")   else None,
                    "h": float(row.high)   if hasattr(row, "high")   else None,
                    "l": float(row.low)    if hasattr(row, "low")    else None,
                    "c": float(row.close)  if hasattr(row, "close")  else None,
                    "v": float(row.volume) if hasattr(row, "volume") else None,
                })
    except Exception as exc:
        logger.debug(f"monitor-data: OHLCV load failed for {name}: {exc}")

    # ── 3. Signals ───────────────────────────────────────────────────────────
    signals: list = []
    if strategy:
        for sig in strategy.get_recent_signals(200):
            signals.append({
                "t": sig.timestamp.isoformat() if hasattr(sig.timestamp, "isoformat") else str(sig.timestamp),
                "type": sig.signal_type.value,
                "price": float(sig.price or 0),
                "strength": float(sig.strength or 0),
                "stop_loss":   float(sig.stop_loss)   if sig.stop_loss   is not None else None,
                "take_profit": float(sig.take_profit) if sig.take_profit is not None else None,
            })

    # ── 4. Equity curve with timestamps ──────────────────────────────────────
    # Rebuild from trade history to get per-trade timestamps (existing
    # _build_strategy_performance() only returns a plain float list).
    equity: list = []
    metrics: dict = {}
    try:
        min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
        risk_report = risk_manager.get_risk_report()
        current_equity = float(((risk_report.get("equity") or {}).get("current") or 0.0))
        config = strategy_manager._configs.get(name)
        equity_base = max(
            min_notional,
            current_equity * float((config.allocation if config else 0) or 0),
        )

        trades = sorted(
            [r for r in risk_manager.get_trade_history(limit=5000)
             if isinstance(r, dict) and str(r.get("strategy") or "").strip() == name],
            key=lambda r: str(r.get("timestamp") or ""),
        )

        mark = equity_base
        realized = 0.0
        equity.append({"t": None, "v": round(mark, 4)})  # baseline point (no timestamp)
        for trade in trades:
            pnl = float(trade.get("pnl") or 0.0)
            ts_raw = str(trade.get("timestamp") or "").strip()
            mark += pnl
            realized += pnl
            equity.append({"t": ts_raw or None, "v": round(mark, 4)})

        unrealized = sum(
            float(p.unrealized_pnl or 0.0)
            for p in position_manager.get_positions_by_strategy(name)
        )
        if equity:
            equity[-1]["v"] = round(mark + unrealized, 4)

        win_count = sum(1 for t in trades if float(t.get("pnl") or 0) > 0)
        metrics = {
            "equity_base":    round(equity_base, 2),
            "realized_pnl":   round(realized, 4),
            "unrealized_pnl": round(unrealized, 4),
            "total_pnl":      round(realized + unrealized, 4),
            "return_pct":     round((realized + unrealized) / equity_base * 100, 3) if equity_base > 0 else 0,
            "trade_count":    len(trades),
            "win_count":      win_count,
            "win_rate":       round(win_count / len(trades) * 100, 1) if trades else None,
        }
    except Exception as exc:
        logger.debug(f"monitor-data: equity curve failed for {name}: {exc}")

    # ── 5. Current open positions ─────────────────────────────────────────────
    positions_data: list = []
    try:
        for pos in position_manager.get_positions_by_strategy(name):
            positions_data.append({
                "symbol":            pos.symbol,
                "side":              pos.side,
                "entry_price":       float(pos.entry_price or 0),
                "current_price":     float(pos.current_price or 0),
                "quantity":          float(pos.quantity or 0),
                "unrealized_pnl":    float(pos.unrealized_pnl or 0),
                "unrealized_pnl_pct":float(pos.unrealized_pnl_pct or 0),
                "entry_time":        pos.entry_time.isoformat() if hasattr(pos.entry_time, "isoformat") else str(pos.entry_time),
            })
    except Exception as exc:
        logger.debug(f"monitor-data: positions failed for {name}: {exc}")

    return {
        "name":      name,
        "symbol":    symbol,
        "timeframe": timeframe,
        "is_running":is_running,
        "ohlcv":     ohlcv,
        "signals":   signals,
        "equity":    equity,
        "metrics":   metrics,
        "positions": positions_data,
        "ts":        datetime.now(timezone.utc).isoformat(),
    }


def _timeframe_to_seconds(tf: str) -> int:
    """Convert timeframe string like '15m', '1h', '4h' to seconds."""
    import re as _re
    tf = str(tf or "1h").strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    m = _re.fullmatch(r"(\d+)([smhdw])", tf)
    if m:
        return int(m.group(1)) * units[m.group(2)]
    return 3600
```

---

## Phase 2 — HTML：新增监控面板 card

**文件：`web/templates/index.html`**

定位：找到策略 tab 中 `id="strategy-edit-panel"` 所在的 card（当前约 606~609 行），
在那个 card 的关闭 `</div>` 之后、下一个 `<div class="card wide">` 之前插入：

```html
<!-- 策略实时监控面板 -->
<div class="card wide" id="strategy-monitor-panel" style="display:none">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
        <h3 style="margin:0" id="monitor-panel-title">策略监控</h3>
        <div style="display:flex;gap:8px;align-items:center">
            <span id="monitor-status-badge"
                  style="font-size:11px;padding:2px 8px;border-radius:10px;
                         background:var(--bg-3);color:var(--text-muted)">未选择</span>
            <button class="btn btn-sm btn-primary"
                    id="btn-monitor-refresh"
                    onclick="refreshStrategyMonitor()">刷新</button>
            <button class="btn btn-sm"
                    onclick="closeStrategyMonitor()"
                    style="background:var(--bg-3);color:var(--text-muted)">关闭</button>
        </div>
    </div>
    <div id="monitor-metrics-row"
         style="display:flex;gap:16px;margin-bottom:10px;flex-wrap:wrap"></div>
    <div id="strategy-monitor-chart" style="width:100%;height:520px"></div>
    <div id="monitor-positions-row" style="margin-top:8px"></div>
</div>
```

---

## Phase 3 — JavaScript

**文件：`web/static/js/app.js`**

### 3.1 在 `openEditor(name)` 中追加一行调用

找到 `openEditor` 函数体（当前约 1204 行）。
在 `panel.classList.add('strategy-edit-active')` 这行**之后**追加：

```javascript
// 同步激活监控面板
openStrategyMonitor(name).catch(() => {});
```

不替换 `openEditor` 中任何现有逻辑。

### 3.2 在文件末尾追加以下代码块

```javascript
// ── 策略实时监控面板 ──────────────────────────────────────────────────────────

let _monitorTimer = null;
let _monitorCurrentName = '';

async function openStrategyMonitor(name) {
    const panel = document.getElementById('strategy-monitor-panel');
    if (!panel) return;
    _monitorCurrentName = String(name || '');
    panel.style.display = 'block';
    setTimeout(() => panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 80);
    await _loadMonitorData(name);
    if (_monitorTimer) clearInterval(_monitorTimer);
    _monitorTimer = setInterval(() => {
        if (_monitorCurrentName) _loadMonitorData(_monitorCurrentName).catch(() => {});
    }, 30000);
}

function closeStrategyMonitor() {
    const panel = document.getElementById('strategy-monitor-panel');
    if (panel) panel.style.display = 'none';
    if (_monitorTimer) { clearInterval(_monitorTimer); _monitorTimer = null; }
    _monitorCurrentName = '';
}

async function refreshStrategyMonitor() {
    if (_monitorCurrentName) await _loadMonitorData(_monitorCurrentName);
}

async function _loadMonitorData(name) {
    const badge = document.getElementById('monitor-status-badge');
    const title = document.getElementById('monitor-panel-title');
    if (badge) badge.textContent = '加载中...';

    let data;
    try {
        data = await api(`/strategies/${encodeURIComponent(name)}/monitor-data?bars=200`);
    } catch (e) {
        if (badge) {
            badge.textContent = '加载失败';
            badge.style.background = '#7f1d1d';
            badge.style.color = '#fca5a5';
        }
        return;
    }

    // 标题与状态徽章
    if (title) title.textContent = `策略监控 — ${esc(name)}`;
    if (badge) {
        badge.textContent = data.is_running ? '运行中' : '未运行';
        badge.style.background = data.is_running ? '#14532d' : 'var(--bg-3)';
        badge.style.color     = data.is_running ? '#86efac' : 'var(--text-muted)';
    }

    // 指标行
    const metricsEl = document.getElementById('monitor-metrics-row');
    if (metricsEl) {
        const m = data.metrics || {};
        const pnlColor = c => (c || 0) >= 0 ? '#4ade80' : '#f87171';
        const pct = v => v != null ? (v > 0 ? '+' : '') + v.toFixed(2) + '%' : 'N/A';
        metricsEl.innerHTML = [
            ['资本基数',   m.equity_base    != null ? m.equity_base.toFixed(2) + ' U' : '--'],
            ['已实现盈亏', m.realized_pnl   != null ? `<span style="color:${pnlColor(m.realized_pnl)}">${m.realized_pnl.toFixed(2)} U</span>` : '--'],
            ['浮动盈亏',   m.unrealized_pnl != null ? `<span style="color:${pnlColor(m.unrealized_pnl)}">${m.unrealized_pnl.toFixed(2)} U</span>` : '--'],
            ['总收益',     m.total_pnl      != null ? `<span style="color:${pnlColor(m.total_pnl)}">${m.total_pnl.toFixed(2)} U (${pct(m.return_pct)})</span>` : '--'],
            ['交易次数',   m.trade_count    != null ? m.trade_count : '--'],
            ['胜率',       m.win_rate       != null ? m.win_rate.toFixed(1) + '%' : '--'],
        ].map(([label, val]) =>
            `<div class="monitor-metric">
               <div class="monitor-metric-label">${label}</div>
               <div class="monitor-metric-val">${val}</div>
             </div>`
        ).join('');
    }

    // 图表
    _renderMonitorChart(data);

    // 持仓行
    const posEl = document.getElementById('monitor-positions-row');
    if (posEl) {
        if (!data.positions || !data.positions.length) {
            posEl.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:4px 0">当前无持仓</div>';
        } else {
            posEl.innerHTML =
                '<div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">当前持仓</div>' +
                data.positions.map(p => {
                    const c = p.unrealized_pnl >= 0 ? '#4ade80' : '#f87171';
                    return `<div class="monitor-position-row">
                        <span class="monitor-pos-side ${p.side === 'long' ? 'pos-long' : 'pos-short'}">${p.side === 'long' ? '多' : '空'}</span>
                        <span>${esc(p.symbol)}</span>
                        <span>入场 ${p.entry_price.toFixed(4)}</span>
                        <span>现价 ${p.current_price.toFixed(4)}</span>
                        <span>数量 ${p.quantity.toFixed(6)}</span>
                        <span style="color:${c}">浮盈 ${p.unrealized_pnl.toFixed(2)} U (${(p.unrealized_pnl_pct*100).toFixed(2)}%)</span>
                    </div>`;
                }).join('');
        }
    }
}

function _renderMonitorChart(data) {
    const chartEl = document.getElementById('strategy-monitor-chart');
    if (!chartEl || typeof Plotly === 'undefined') return;

    const ohlcv   = data.ohlcv   || [];
    const signals = data.signals || [];
    const equity  = data.equity  || [];

    // K 线
    const candleTrace = {
        type: 'candlestick',
        name: data.symbol || '',
        x:     ohlcv.map(b => b.t),
        open:  ohlcv.map(b => b.o),
        high:  ohlcv.map(b => b.h),
        low:   ohlcv.map(b => b.l),
        close: ohlcv.map(b => b.c),
        increasing: { line: { color: '#4ade80', width: 1 }, fillcolor: '#4ade80' },
        decreasing: { line: { color: '#f87171', width: 1 }, fillcolor: '#f87171' },
        xaxis: 'x', yaxis: 'y',
    };

    // 买入信号（上三角）
    const buySigs  = signals.filter(s => ['buy',  'close_short'].includes(s.type));
    const sellSigs = signals.filter(s => ['sell', 'close_long' ].includes(s.type));

    const buyMarker = {
        type: 'scatter', mode: 'markers', name: '买入/平空',
        x: buySigs.map(s => s.t),
        y: buySigs.map(s => s.price),
        marker: {
            symbol: 'triangle-up',
            size:   buySigs.map(s => 8 + (s.strength || 0.5) * 8),
            color:  '#4ade80',
            line:   { color: '#fff', width: 1 },
        },
        text: buySigs.map(s =>
            `${s.type} | 强度 ${(s.strength||0).toFixed(2)}`
            + (s.stop_loss    ? ` | SL ${s.stop_loss.toFixed(4)}`    : '')
            + (s.take_profit  ? ` | TP ${s.take_profit.toFixed(4)}`  : '')
        ),
        hovertemplate: '%{text}<br>价格: %{y}<br>时间: %{x}<extra></extra>',
        xaxis: 'x', yaxis: 'y',
    };

    // 卖出信号（下三角）
    const sellMarker = {
        type: 'scatter', mode: 'markers', name: '卖出/平多',
        x: sellSigs.map(s => s.t),
        y: sellSigs.map(s => s.price),
        marker: {
            symbol: 'triangle-down',
            size:   sellSigs.map(s => 8 + (s.strength || 0.5) * 8),
            color:  '#f87171',
            line:   { color: '#fff', width: 1 },
        },
        text: sellSigs.map(s => `${s.type} | 强度 ${(s.strength||0).toFixed(2)}`),
        hovertemplate: '%{text}<br>价格: %{y}<br>时间: %{x}<extra></extra>',
        xaxis: 'x', yaxis: 'y',
    };

    // 权益曲线（只保留有时间戳的点）
    const equityPts = equity.filter(e => e.t);
    const equityTrace = {
        type: 'scatter', mode: 'lines', name: '权益曲线',
        x: equityPts.map(e => e.t),
        y: equityPts.map(e => e.v),
        line:      { color: '#60a5fa', width: 1.5 },
        fill:      'tozeroy',
        fillcolor: 'rgba(96,165,250,0.08)',
        hovertemplate: '权益: %{y:.2f} U<br>时间: %{x}<extra></extra>',
        xaxis: 'x', yaxis: 'y2',
    };

    const hasEquity = equityPts.length > 0;

    const layout = {
        paper_bgcolor: 'transparent',
        plot_bgcolor:  'transparent',
        font:   { color: '#dfe9f7', size: 11 },
        margin: { t: 16, b: 40, l: 60, r: 40 },
        xaxis: {
            ...plotlyTimeAxis(),
            domain: [0, 1],
            anchor: 'y',
            rangeslider: { visible: false },
        },
        yaxis: {
            domain:    hasEquity ? [0.32, 1] : [0, 1],
            gridcolor: '#283242',
            title:     { text: '价格', font: { size: 10 } },
        },
        xaxis2: hasEquity ? { ...plotlyTimeAxis(), domain: [0, 1], anchor: 'y2' } : undefined,
        yaxis2: hasEquity ? {
            domain:    [0, 0.28],
            gridcolor: '#283242',
            title:     { text: '权益(U)', font: { size: 10 } },
        } : undefined,
        showlegend: true,
        legend: { orientation: 'h', y: 1.04, x: 0, font: { size: 10 } },
    };

    const traces = [candleTrace, buyMarker, sellMarker];
    if (hasEquity) traces.push(equityTrace);

    try {
        Plotly.react(chartEl, traces, layout, {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['select2d', 'lasso2d'],
            displaylogo: false,
        });
    } catch (e) {
        chartEl.innerHTML = `<div style="color:var(--text-muted);padding:20px;text-align:center">
            图表渲染失败: ${esc(e.message)}</div>`;
    }
}
```

---

## Phase 4 — CSS

**文件：`web/static/css/style.css`**

在文件**末尾**追加：

```css
/* ── 策略实时监控面板 ── */
#strategy-monitor-panel { transition: none; }

.monitor-metric {
    display: flex;
    flex-direction: column;
    min-width: 90px;
    background: var(--bg-2);
    border-radius: 6px;
    padding: 6px 10px;
}
.monitor-metric-label {
    font-size: 10px;
    color: var(--text-muted);
    margin-bottom: 2px;
}
.monitor-metric-val {
    font-size: 13px;
    font-weight: 600;
}
.monitor-position-row {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
    font-size: 11px;
    padding: 4px 8px;
    background: var(--bg-2);
    border-radius: 5px;
    margin-bottom: 4px;
}
.monitor-pos-side {
    font-weight: 700;
    font-size: 12px;
    padding: 1px 6px;
    border-radius: 4px;
}
.pos-long  { background: #14532d; color: #86efac; }
.pos-short { background: #7f1d1d; color: #fca5a5; }
```

---

## 验收测试

```
1. 启动服务，打开策略页面，确保至少有一个已注册的策略实例。

2. 点击任意策略卡片：
   - 策略参数编辑面板正常弹出（原有功能不受影响）
   - 编辑面板下方自动出现"策略监控"card

3. 图表验证：
   - 上方：K 线蜡烛图
   - 上方叠加：买入信号（绿色向上三角）/ 卖出信号（红色向下三角），三角大小与信号强度正相关
   - 下方（有交易记录时）：权益曲线折线图
   - Hover 显示价格/时间/信号类型/强度/止损止盈

4. 指标行验证：
   - 显示资本基数、已实现盈亏、浮动盈亏、总收益(%)、交易次数、胜率

5. 持仓行验证：
   - 有持仓时显示多/空标签 + 入场价/现价/浮盈 (USDT + %)
   - 无持仓时显示"当前无持仓"

6. 自动刷新验证：
   - 保持页面，策略产生新信号后 ≤30 秒图表自动更新

7. 边界情况：
   - 新注册但未运行的策略：K 线正常显示，无信号标记，指标全为 0
   - 无历史 K 线缓存：图表区域空白，不报 JS 错误
   - 无交易历史：不显示权益子图，K 线图占满全高

8. 点击"关闭"按钮：
   - 监控面板隐藏，自动刷新定时器停止
```

---

## 已知局限与后续改进方向

1. **Shadow 策略无数据**：需先修复 `core/deployment/promotion_engine.py:259`，将 `started: False` 改为实际注册并启动策略，shadow 候选才能在此面板显示数据。

2. **权益曲线基线无时间戳**：第一个点（初始资本）无交易时间，图中不显示；若希望从策略启动时间开始画线，需在 `_running_since` 中读取启动时刻作为第一个点的时间戳。

3. **OHLCV 数据依赖本地缓存**：若 `data_storage` 无该标的/周期的本地 parquet，则 K 线为空。建议提示用户先在「行情查看」页面下载历史数据。

4. **信号数量上限 200 条**：内存中最多保留 1000 条（`strategy_base.py:210`），请求 200 条，长时间运行后只能看到最近信号。如需完整历史，需将信号写入 SQLite。

5. **多标的策略**：当前只取 `symbols[0]` 的 K 线；多标的策略建议在面板添加 symbol 切换器。
