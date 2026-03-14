# AI 研究与决策部署现状梳理（2026-03-14）

## 范围
- 仓库路径：`E:\9_Crypto\crypto_trading_system`
- 本次基于代码与测试结果梳理，并保存结论。
- 说明：检查时本机 `127.0.0.1:8000` 未在监听，因此未做在线接口联调。

## 已确认能力
- AI 研究工作流完整：支持 `proposal` 生成、运行、候选产出、生命周期追踪。
- 决策/部署链路完整：支持 `promote/register/human-approve/quick-register`。
- AI 决策与自治代理配置完整：有 runtime config、run-once、状态查询、journal。

## 关键证据（代码）
- 研究运行入口：`web/api/ai_research.py` 的 `POST /proposals/{proposal_id}/run`
- 候选注册入口：`web/api/ai_research.py` 的 `POST /candidates/{candidate_id}/register`
- 治理模式下人工审批入口：`web/api/ai_research.py` 的 `POST /candidates/{candidate_id}/human-approve`
- 治理模式下快速纸盘注册：`web/api/ai_research.py` 的 `POST /candidates/{candidate_id}/quick-register`
- 治理门控：`core/research/orchestrator.py` 的 `promote_existing_candidate` 在 `GOVERNANCE_ENABLED=true` 时返回 409
- 研究完成后门控标记：`core/research/orchestrator.py` 会设置 `promotion_pending_human_gate`

## 关键证据（测试）
- 执行：
  - `pytest -q tests/test_ai_research_autonomous_agent_api.py tests/test_ai_research_runtime_and_phase_e.py tests/ops/test_ops_ai_proposals.py tests/ops/test_ops_research_jobs.py`
- 结果：
  - `36 passed, 1 warning`

## “能不能一键研究和决策部署”结论
- 结论 1：可以“一键串联”，但默认前端不是单按钮原子动作。
- 结论 2：当前产品形态更像“分步一键”：
  - 研究：生成研究 -> 运行研究
  - 部署：候选注册（或人工批准/快速注册）
- 结论 3：治理开关决定一键方式：
  - `GOVERNANCE_ENABLED=false`：可直接 `/register`
  - `GOVERNANCE_ENABLED=true`：需走 `quick-register` 或 `human-approve`

## 当前配置观察
- `config/settings.py` 默认 `GOVERNANCE_ENABLED=False`
- 你的 `.env` 里 `TRADING_MODE=paper`
- 因此在当前默认配置下，候选可直接注册到纸盘运行

## 本次新增：CLI 一键编排脚本
- 文件：`scripts/oneclick_ai_research_deploy.py`
- 作用：一条命令执行“生成研究 -> 运行 -> 按治理模式自动走注册/审批”

示例：

```powershell
python scripts/oneclick_ai_research_deploy.py `
  --goal "研究 BTC 趋势延续与回撤修复策略" `
  --symbols BTC/USDT `
  --timeframes 15m,1h `
  --days 30
```

仅研究不部署：

```powershell
python scripts/oneclick_ai_research_deploy.py --goal "研究 ETH 短周期策略" --skip-deploy
```

## 风险与建议
- 若切到 `live`，仍需遵守治理审批与风控限制，脚本不会绕过高权限门禁。
- 建议先在 `paper` 跑通后，再考虑实盘候选与 live 流程。
