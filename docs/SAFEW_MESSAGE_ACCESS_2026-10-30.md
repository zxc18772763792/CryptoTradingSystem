# SafeW 默认通知会话接入试验

日期：2026-10-30，Asia/Shanghai。

## 已验证结果

- SafeW Desktop 3.9.1 的默认认证会话为 `SafeW / service notifications`。
- 桌面客户端支持单会话导出为 JSON。本次导出成功，包含 6 条消息，其中 1 条带图片元数据；未下载媒体内容。
- 导出确认 `type=personal_chat`、`id=777000`。这是系统服务通知会话，不是能邀请采集机器人的群频道。
- 使用项目 Python 执行 `scripts/probe_safew_export.py`，首次导入新增 6 个版本；再次导入同一文件新增 0、去重 6，总消息数仍为 6。
- 对来源限制、消息身份、时间戳、正文不落盘、富文本、版本去重及失败不产生状态文件的 14 个测试通过。

## 接入边界

本次已验证 **桌面历史 JSON → 本地只读探针状态**。尚未验证用户账号 API、无人值守自动抓取、推送接收或连续消息覆盖。

默认系统会话可能包含验证码、设备和登录信息。探针在内存中解析正文，仅持久化消息 ID、发布时间、首次导入时间、内容指纹及媒体存在标记；正文、发送者详情和媒体路径不落入探针状态，也不发送给模型。默认源固定为 `777000`，不扩展到其他群或账号。

所有记录标记 `historical_desktop_export`、`strategy_eligible=false`；状态中的 `live_connection_verified` 和 `continuous_coverage_verified` 均为 `false`。不会产生交易信号或调用交易接口。媒体仅有元数据不代表已读取图片内容。

发布时间使用导出的 `date_unixtime` 转为 UTC，不猜测缺少时区的 `date`；首次观察时间是实际导入时刻，不回填为发帖时间。相同消息不同正文、编辑时间或媒体存在标记产生新版本。

## 复用方式

在项目根目录执行，替换为实际单会话导出文件路径：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' `
    scripts/probe_safew_export.py 'C:\path\to\SafeW\result.json'
```

默认本地状态和本次两份测试回执位于 Git 忽略的目录：

- `runtime/safew_probe/status.json`
- `runtime/safew_probe/first_import.json`
- `runtime/safew_probe/repeated_import.json`

原始 JSON 保留在桌面客户端选择的本机导出目录，可能含登录信息，不应提交仓库或发送给模型。

策略来源接入仍需选定实际群或频道，并验证机器人权限及真实更新，或单独完成个人账号读取的可行性验证。不能把本次历史导入、API 入口可达或界面可见当作实时接入已经完成。
