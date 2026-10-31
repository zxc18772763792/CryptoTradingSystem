# 币安中文群情绪指标优化

2026-10-31，Asia/Shanghai。范围：研究跟踪器的观察、覆盖诊断与采集 busy 重试；纸面交易规则延续原协议。

## 变化

- 主表增加小时／6小时窗口摘要，展开后显示观察分、独立作者、未触发原因、窗口时点和最早可评估的完整窗口。
- 观察分为原冻结窗口 `imbalance × 100`，范围 −100 至 +100，按原作者等权口径；它不是涨跌概率。少于5作者的观察分保持探索性，缺观点、缺记录、覆盖缺口、采集异常分别表示。
- 历史取 `observations.sqlite` 中已记录的窗口，不从当前标签重建历史。SQLite 以只读模式打开，查询上限为168个小时窗口、28个六小时窗口，并以最新已完成 cycle 为时间上界。
- 展示四本账本各自的冻结、结算、待结算、净收益、同入场做多超额、40bp压力及胜率。主表仍使用小时顺势账本，空样本收益和胜率为 null，四账本不相加为独立样本。
- 明确来源成员状态、完整批次、连续覆盖起点；未知覆盖和未来采集时间不能表示健康。
- 采集 busy 后最多两次15秒短重试，持续 busy 再回到300秒周期；每次子进程退出后才重试，保留原 reader/study 锁、冷却和停止信号。

## 诊断依据与限制

09:48 北京时间的快照包含86个小时窗口（69个无有效观点、17个作者不足）及4个六小时窗口（1个无有效观点、3个作者不足），没有达标窗口。六小时最新应评估窗口跨越来源连续覆盖起点，显示覆盖不足，并提示后续完整窗口时点。这解释了空账本，不构成交易有效性结论。

窗口达标只是情绪门槛；延迟、持仓重叠等其他条件与模拟交易结果仍以原冻结交易账为准。本轮没有调整5作者、0.4不平衡、6小时持有、成本或文本分类规则。

已校验激活记录中的实现与协议 SHA：冻结 CLI、核心、桥接器及分类／复核规则保持原指纹。正常 cycle 的消息、状态和 journal 继续由已有任务更新。

## 验证

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' -m pytest tests/test_telegram_chat_sentiment.py tests/test_telegram_forward_tracker.py tests/test_tracker_registry.py tests/test_tracker_measurement.py -q
node --test tests/research_tracker_diagnostics.test.cjs
node scripts/verify_binance_chat_sentiment_ui.cjs
```

后端95项、前端12项通过。浏览器检查覆盖实际 API、两张观察卡、四账本、历史展开状态保留、1440px和430px布局及无未捕获JS错误。验证回执和截图在 Git 忽略的 `data/research/binance_chat_sentiment_20261031/ui/`。430px页面已有导航条4px溢出；展开本指标不增加页面宽度，指标卡完整处于视口内。

部署使用 `web.bat stop` / `web.bat start -PaperMode`，仅重载 Web，已有新闻与 LLM worker 保持运行；没有推送远程仓库。
