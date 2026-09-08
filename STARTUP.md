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

### PowerShell 启动时报 `Key in dictionary: 'PATH' ... 'Path'`

这是 PowerShell 7 在 Windows 上继承大小写不同的 PATH 别名导致的启动器问题。项目启动脚本会在加载 `.env` 后自动规范化环境变量，保留 canonical `Path` 后再创建 Web、Worker 和 Supervisor 进程。若使用旧脚本缓存，重新打开 PowerShell 后再次执行：

```powershell
.\web.bat stop -IncludeWorkers
.\web.bat start
```

启动失败时先查看 `logs\web_ps.log`、`logs\web_supervisor.err.log` 和最新的 `logs\uvicorn_web_*.err.log`。

### 8000 端口被非托管进程占用

`web.bat stop` 会拒绝终止无法确认归属的进程。先确认端口 PID 是本项目的 Python/Uvicorn 进程，再手动结束后重启：

```powershell
Get-Process -Id <PID> | Select-Object Id,ProcessName,Path
Stop-Process -Id <PID> -Force
.\web.bat start
```

- Web 日志：`logs\uvicorn_web_*.out.log`、`logs\uvicorn_web_*.err.log`
- Supervisor 日志：`logs\web_supervisor.log`
- 环境检查：`.\scripts\setup_local_env.ps1 -SkipRequirements`
- 服务检查：`.\web.bat status`

### 启动即失败（服务根本起不来）

先跑环境检查，它区分"必需"和"可选"两类依赖：

```powershell
& F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe scripts\verify_env.py
```

- 报 **MISSING REQUIRED MODULES** → 服务无法启动，按提示装依赖后重试。
  这类模块是启动路径上的无保护顶层 import（例如 `web/main.py:23` 的 `Jinja2Templates`
  依赖 `jinja2`，而它是 FastAPI 的可选 extra，pip 不会自动带入）。
- 只报 `[WARN] optional` → 服务能起，但对应功能已静默降级，见下。

### 功能"没反应"但服务是好的

可选依赖缺失不会阻止启动，只会安静地削掉功能。最容易误诊的是 ML：
缺 `xgboost` / `scikit-learn` 时，ML 信号恒为 FLAT、权重归零，
而聚合器**照常输出交易决策**，提示只有启动时的一行 WARNING
（`core/ai/ml_signal.py`）。排查"模型为什么不出信号"时先看这里。

`verify_env.py` 会列出所有缺失的可选依赖及其影响；原生包（ta-lib、matplotlib、
polars、asyncpg 等）请用 conda-forge 安装，不要用 pip，理由见 `AGENTS.md`。
