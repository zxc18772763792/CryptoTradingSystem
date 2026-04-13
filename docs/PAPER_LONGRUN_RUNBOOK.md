# Paper Longrun Runbook

这份 runbook 面向纸上交易的长时间运行、烟雾自检和故障处理。默认目标是 `paper` 模式，任何涉及 `live` 的启动都应被当成显式变更来处理。

## 1. 启动

推荐从仓库根目录启动：

```bat
.\web.bat start
```

如果需要显式检查纸上模式，先确认环境变量或启动参数没有把系统带到 `live`，再启动：

```powershell
python main.py --mode web --trading-mode paper
```

启动后优先确认：

```bat
.\web.bat status
```

## 2. 停止

正常停止 web 和已观察到的 worker：

```bat
.\web.bat stop -IncludeWorkers
```

如果服务状态异常、端口占用或长时间卡住，优先先停再重启，不要在未知状态下继续堆叠新进程。

## 3. 重启

推荐的干净重启流程：

```bat
.\web.bat stop -IncludeWorkers
.\web.bat start
```

如果你只需要验证一次性自检，不必重启整个环境时，可以直接执行：

```powershell
python scripts/selfcheck_paper_longrun.py --base-url http://127.0.0.1:8000 --token $env:OPS_TOKEN
```

## 4. 自检

建议在每次启动后、以及长跑期间做周期性自检：

```powershell
python scripts/selfcheck_paper_longrun.py `
  --base-url http://127.0.0.1:8000 `
  --token $env:OPS_TOKEN `
  --timeout 12
```

脚本会检查：

- 进程/API 是否可达
- `/health` 和 `/api/status` 是否返回有效状态字段
- `paper` 安全态是否成立
- `/ops/health` 和 `/ops/status` 是否正常
- run-once 端点是否可以返回

脚本会输出：

- 终端可读摘要
- 结构化 JSON 报告

失败时退出码非 `0`，适合接 CI、定时任务或人工 smoke check。

## 5. 日志与状态文件

常用路径：

- `logs/web_ps.log`
- `logs/trading_YYYY-MM-DD.log`
- `logs/`
- `runtime/`
- `data/`

排障时优先看：

1. `logs/web_ps.log`
2. `logs/trading_YYYY-MM-DD.log`
3. 服务进程是否仍在监听端口
4. `.\web.bat status` 的输出

## 6. 故障排查

### 6.1 服务起不来

先确认：

- 端口是否已被占用
- `.env` 或 `.env.local` 是否覆盖了错误的启动参数
- `OPS_TOKEN` 是否配置
- `TRADING_MODE` 是否被意外设成 `live`

建议顺序：

1. `.\web.bat status`
2. 查看 `logs/web_ps.log`
3. 查看 `logs/trading_YYYY-MM-DD.log`
4. `.\web.bat stop -IncludeWorkers`
5. 重新 `.\web.bat start`

### 6.2 自检显示 unsafe state

如果 `paper_trading=false` 或 `trading_halted=true`，先不要继续跑长线：

1. 确认当前模式是否为 `paper`
2. 检查风险管理是否触发了停机
3. 检查是否有手工操作或历史状态把系统切到了 `live`
4. 必要时先停机，再按纸上模式重新启动

### 6.3 run-once 端点失败

如果 `worker_run_once` 返回错误：

1. 先确认 API 认证头是否正确
2. 检查后端数据库和新闻数据源是否可达
3. 复查 `logs/trading_YYYY-MM-DD.log` 和相关 worker 日志
4. 重新跑一次 `selfcheck_paper_longrun.py`

### 6.4 端口可达但状态不对

常见原因是：

- 旧进程残留
- worker 没有按预期启动
- 启动时加载了错误的环境变量
- 运行中的风险状态被触发

建议先停机，再用一条干净的 `.\web.bat start` 重新拉起。

## 7. 运行建议

- 默认只跑 `paper`
- 任何 `live` 恢复都要显式审批和复核
- 长跑期间固定间隔做自检并保留输出 JSON
- 在 burn-in 期内，优先关注错误日志、风险停机、队列堆积和 API 响应时延

