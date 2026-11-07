# 账本日志断尾修复

`event_short_preclose_v1`、`unlock_t7_big_v1`、`korea_frozen_skip_v1` 三个账本的日志是追加写入的哈希链。

## 什么时候需要

进程或磁盘在写入中途出问题，日志最后会留下一行不完整的内容。读日志时这行会被跳过，但下一次追加会直接接在它后面，之后每次读都失败。跟踪器页面显示“日志校验失败”，这个账本停止记账。

这是失败即停，不会算错数。这三个账本的模块已按哈希锁定，不能改代码，所以修复放在单独的脚本里。

## 怎么修

1. 停网页服务：`.\web.bat stop`。
2. 先预览，不会改任何东西：

   ```bash
   python scripts/repair_ledger_journal_tail.py event_short_preclose_v1
   ```

3. 确认输出的残片数量和字节数都很小，记录数也对得上，再写入：

   ```bash
   python scripts/repair_ledger_journal_tail.py event_short_preclose_v1 --apply
   ```

4. 重启服务：`.\web.bat start`，确认页面不再显示“日志校验失败”。

`python` 指项目解释器，见 `AGENTS.md` 第 1 节。

## 脚本保证什么

- 只删完整记录之外的字节，完整记录逐字节保留。
- 结果必须通过账本自己的链校验，否则退出码为 1，不写入。这说明损坏不只是断尾，需要从备份恢复。
- `journal.lock` 存在时退出码为 2，不写入。锁可能是崩溃遗留的：确认服务已停后，手动删除这个锁文件再运行。
- 原文件保留为同目录的 `journal.jsonl.before_repair_<时间>`。

丢掉的那条残片本来就没写完。如果它是一次入场或结算，账本下一轮会按自己的规则重新处理或判为错过，脚本不会补写。

其他账本的日志读写方式不同（比如 `hub_runup_filter_v1` 读到残行当场就报错），不在这个脚本的范围内。
