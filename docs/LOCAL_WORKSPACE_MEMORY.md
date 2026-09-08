# 本地工作记忆

## 浏览器自动化 / Playwright

- 当前环境提供 Playwright，不应再将其误报为“缺少 Playwright”。
- Codex bundled runtime 路径：`C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\node_modules`。
- 已确认可用包：`playwright`、`playwright-core`，当前版本为 `playwright@1.62.1`。
- 当前仓库没有本地 `node_modules`，也没有 `package.json`；直接运行 `npx playwright test` 可能临时安装另一套 CLI。
- `tests/playwright_smoke.spec.js` 当前使用 `require('@playwright/test')`，这与已提供的 `playwright` 包不是同一个入口。运行 smoke 前应先确认测试 runner 入口和 bundled runtime，而不是据此判断环境缺少 Playwright。
- Codex 浏览器交互优先使用已连接的 Chrome/CUA；需要命令行自动化时，优先显式使用 bundled Node/runtime 和实际存在的 Playwright 包。

## 纠错记录

2026-09-07：曾因 `@playwright/test` 解析失败误报“当前环境缺少 Playwright”。实际是依赖入口不匹配，后续必须区分“Playwright 可用”和“项目测试 runner 依赖未配置”。
