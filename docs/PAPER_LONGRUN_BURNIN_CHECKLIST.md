# Paper Longrun Burn-In Checklist

> 目标：在进入长时间 paper 运行前，先用最小风险方式验证服务、状态和 run-once 链路。

## 1. 基本信息

- 日期：
- 负责人：
- 环境：
- 启动方式：
- `base_url`：
- `OPS_TOKEN`：
- 预计 burn-in 时长：
- 备注：

## 2. 启动前

- [ ] 已确认当前目标是 `paper`，不是 `live`
- [ ] 已检查 `.env` / `.env.local` 没有覆盖成错误的运行参数
- [ ] 已确认 `logs/`、`runtime/`、`data/` 目录可写
- [ ] 已确认 `OPS_TOKEN` 已配置
- [ ] 已确认没有未解释的旧进程占用端口

## 3. 启动后 5 分钟内

- [ ] `.\web.bat status` 正常
- [ ] `python scripts/selfcheck_paper_longrun.py --base-url http://127.0.0.1:8000 --token %OPS_TOKEN%` 返回 `0`
- [ ] `/health` 返回可达
- [ ] `/api/status` 显示 `running`
- [ ] `paper_trading=true`
- [ ] `risk_trading_halted=false`
- [ ] `/ops/status` 显示 `execution_mode=paper`
- [ ] run-once 端点返回成功

## 4. 运行中检查

建议每隔一段时间重复一次自检，并记录结果：

- 时间点：
- 结果：
- 是否有新错误日志：
- 是否出现风险停机：
- 是否出现队列堆积：
- 是否出现 API 超时：

可按下面模板填：

| 时间 | selfcheck | paper_trading | risk_halted | execution_mode | 备注 |
| --- | --- | --- | --- | --- | --- |
|  | PASS / FAIL | true / false | true / false | paper / live / unknown |  |
|  | PASS / FAIL | true / false | true / false | paper / live / unknown |  |
|  | PASS / FAIL | true / false | true / false | paper / live / unknown |  |

## 5. 异常处理

- [ ] 如出现 `paper_trading=false`，先暂停长跑并复核启动参数
- [ ] 如出现 `risk_trading_halted=true`，先查风险原因，再决定是否恢复
- [ ] 如出现 `worker_run_once` 失败，先查认证、数据库和日志
- [ ] 如出现连续超时，检查端口、网络和服务主进程
- [ ] 如需要重启，先 `.\web.bat stop -IncludeWorkers` 再 `.\web.bat start`

## 6. 收尾

- [ ] 已保存自检 JSON 输出
- [ ] 已记录 burn-in 期间的异常和处理动作
- [ ] 已确认是否可以继续长跑
- [ ] 已确认不需要切换到 `live`

