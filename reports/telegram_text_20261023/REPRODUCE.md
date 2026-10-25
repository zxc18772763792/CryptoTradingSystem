# 复跑说明

所有命令从项目根目录执行，使用项目已有 conda 环境。无需新的凭据。

纯离线价格回测（使用本次已冻结的标签、复核结果和行情副本）：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts/research_telegram_text.py backtest --market-root data/research/cryptoquant_text_20261023/market_snapshot
```

重建图表、报告和执行审计笔记本时，通过 conda 激活正确的原生 DLL 路径：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\Scripts\conda.exe' run -p 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading' --no-capture-output 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts/build_telegram_text_report.py
```

本次已验证上述报告生成和笔记本内的两组24h结果复算。直接从未激活的进程启动绘图会遇到 Matplotlib 原生 DLL 异常；未改动依赖版本。

中文群 JSON 收到后应使用独立研究输出目录，保留当前冻结研究文件。导入器仅接受单个聊天 JSON，丢弃姓名等作者字段并对作者ID使用同一研究目录的随机盐作去标识化。不要导入整个账号资料目录。
