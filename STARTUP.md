# 本地启动说明

项目使用仓库外层的固定本地环境，不会扫描或修改用户目录下的 Conda、Python 环境。

## 第一次准备

在 `F:\9_Crypto\crypto_trading_system` 执行：

```powershell
.\scripts\setup_local_env.ps1
```

脚本按以下顺序处理：

1. 使用 `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading`（已有时直接复用）。
2. 若本地 Miniforge 存在但环境不存在，则按 `environment.yml` 创建到同一目录。
3. 若本地 Miniforge 也不存在，则在项目目录创建 `.venv`，并安装 `requirements.txt`。

所有依赖、虚拟环境和运行时文件都留在 `F:\9_Crypto` 项目目录内。

## 日常启动

```powershell
.\web.bat
```

这条命令会启动 Web 服务、新闻 worker 和新闻 LLM worker，并打开 Chrome。默认模式为 paper，地址是 <http://127.0.0.1:8000>。

常用命令：

```powershell
.\web.bat start       # 启动，不打开浏览器
.\web.bat status      # 查看服务和 worker 状态
.\web.bat stop        # 停止 Web 服务
.\web.bat stop -IncludeWorkers
                       # 同时停止新闻/LLM/PM worker
```

需要重装或更新依赖时：

```powershell
.\scripts\setup_local_env.ps1 -Upgrade
```

## 模式与可选组件

```powershell
.\web.bat start -PaperMode
.\web.bat start -AllowPersistedLiveMode
.\web.bat start -StartAutonomousAgent
.\web.bat start -EnableAnalyticsHistory
.\web.bat help
```

`-AllowPersistedLiveMode` 只用于明确的 live 启动；切换 worker 组合前先执行 `stop -IncludeWorkers`。启动脚本固定读取项目内环境，找不到时会直接提示运行 `setup_local_env.ps1`，不会继续尝试系统 Python。

## 故障定位

- Web 日志：`logs\uvicorn_web_*.out.log`、`logs\uvicorn_web_*.err.log`
- Supervisor 日志：`logs\web_supervisor.log`
- 环境检查：`.\scripts\setup_local_env.ps1 -SkipRequirements`
- 服务检查：`.\web.bat status`
