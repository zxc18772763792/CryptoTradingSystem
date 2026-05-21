# Exit Logic Verification (2026-05-21)

## Phase 1 Backtest Alignment

| Scenario | Trades | Close Trades |
|---|---:|---:|
| Legacy protective exits disabled | 0 | 0 |
| Current protective exits enabled | 8 | 8 |

- Trade count delta: `8` (`800.0%`)
- Acceptance `>=30%`: `PASS`

## Phase 1/2/3/4 Exit Reason Coverage

| Exit Reason | Trades | Share |
|---|---:|---:|
| `signal_close` | 1 | 20.0% |
| `stop_loss` | 1 | 20.0% |
| `take_profit` | 1 | 20.0% |
| `time_stop` | 1 | 20.0% |
| `trailing_stop` | 1 | 20.0% |

- Required reasons present: `PASS`
- Missing reasons: `NONE`
- `trailing_stop` share: `20.0%`
- `signal_close` share: `20.0%`
- `time_stop` average net PnL: `0.000000`

## Phase 2 Profit Management Defaults

| Metric | Value |
|---|---:|
| `profit_management_atr_pct` | 0.0100 |
| `profit_protect_trigger_pct` | 0.0100 |
| `profit_protect_lock_pct` | 0.0010 |
| `partial_take_profit_trigger_pct` | 0.0150 |
| `post_partial_trailing_activation_pct` | 0.0200 |

- Acceptance 1x/1.5x/2x ATR defaults: `PASS`

## Local 30-Day BTC/USDT 1h Bollinger Backtest

| Exit Reason | Trades | Share |
|---|---:|---:|
| `bollinger_middle_reversion` | 5 | 38.5% |
| `signal_reversal` | 8 | 61.5% |

- Active strategy close share: `38.5%`
- Win rate: `0.6923`
- Acceptance active close share `>=20%`: `PASS`

## Local 30-Day Multi-Strategy SL Share

| Strategy | Closes | Stop Loss | Stop Loss Share |
|---|---:|---:|---:|
| `MAStrategy` | 17 | 0 | 0.0% |
| `RSIStrategy` | 13 | 0 | 0.0% |
| `MACDStrategy` | 35 | 0 | 0.0% |
| `BollingerBandsStrategy` | 13 | 0 | 0.0% |
| `VWAPReversionStrategy` | 7 | 0 | 0.0% |
| `MeanReversionStrategy` | 22 | 0 | 0.0% |
| `MomentumStrategy` | 7 | 0 | 0.0% |

- Aggregate stop_loss share: `0.0%`
- Acceptance stop_loss share `<=30%`: `PASS`

## Phase 7 ATR Protection

| Metric | Value |
|---|---:|
| `atr_pct` | 0.03365385 |
| stop-loss distance / ATR | 1.5000 |
| metadata has `atr_protection_applied` | True |

- Acceptance SL distance about `1.5x ATR`: `PASS`

## Phase 5 Live Maker/Fallback Audit

| Mode | Journal Closes | Share |
|---|---:|---:|
| `unknown` | 10 | 100.0% |

- Known post-change `close_order_mode` samples: `0`
- LIMIT share and average close slippage require post-deploy live close samples; this report verifies instrumentation and keeps the KPI explicit.
