# Web 控制台统一风格指南

> 适用范围：`web/templates/*.html`、`web/static/css/style.css`、`web/static/js/*.js` 中 JS 动态生成的 DOM。
> 原则：**所有颜色一律取自 `style.css` 顶部 `:root` 的全局 token，禁止新增裸 hex / 裸 rgba。**

## 1. 设计基调

深海军蓝金融终端风：纯色面 + 1px 细边框 + 少量渐变点缀。
主强调为蓝（交互/链接/数值），绿=盈利/成功，红=亏损/危险，琥珀=警告/模拟盘，
灰蓝五级文字层次。卡片标题统一带「蓝→绿」渐变短竖条（`.card h3::before`）。

## 2. 全局 token（`style.css` 顶部 `:root`）

### 表面层级（由深到浅）
| token | 值 | 用途 |
|---|---|---|
| `--bg-main` | `#0a1018` | 页面底色 |
| `--bg-secondary` | `#111a26` | 二级底 / 大区块底 |
| `--panel-bg-deep` | `#0e1724` | 输入框、深凹陷面 |
| `--panel-bg` | `#121c2b` | 嵌套面板、列表底 |
| `--card-bg` | `#162232` | 卡片面 |
| `--surface-raised` | `#1b2b41` | 悬浮/hover/激活面 |

### 边框三级
`--border-subtle #24384f`（弱分隔）→ `--card-border #2a3b52`（常规）→ `--border-strong #2f4a68`（强调/激活）

### 文字五级
`--text-bright #eef6ff`（高亮标题/数值）→ `--text-main #e8eef9`（正文）→
`--text-soft #c6d4e8`（次正文）→ `--text-sub #9fb1c9`（说明）→ `--text-faint #7e92b2`（弱说明/占位）

### 语义色（各含 soft / deep 变体）
| 语义 | 常规 | soft（浅文字/图标） | deep（深底/渐变起点） |
|---|---|---|---|
| 强调/交互（蓝） | `--accent #3aa6ff` | `--accent-soft #93c5fd` | — |
| 盈利/成功（绿） | `--positive #20bf78` | `--positive-soft #6fdca4` | `--positive-deep #137f49` |
| 亏损/危险（红） | `--negative #e05260` | `--negative-soft #f58f97` | `--negative-deep #7f1d1d` |
| 警告/模拟（琥珀） | `--warning #f0b429` | `--warning-soft #fbc964` | `--warning-deep #d98c29` |
| 中性/盘整 | `--neutral #555f72` | — | — |
| 信息徽章（钢蓝） | `--steel #4f7c9b` | — | `--steel-deep #31506c` |
| BTC 橙 | `--btc #f7931a` | — | — |

### rgba 组装
半透明一律 `rgba(var(--X-rgb), α)`，可用三元组：
`--accent-rgb` `--positive-rgb` `--negative-rgb` `--warning-rgb` `--btc-rgb`
`--line-rgb`（分隔线/弱边框基色）`--shadow-rgb`（阴影）`--panel-rgb` `--raised-rgb`（半透明面）。

## 3. 用法规则

1. **新样式禁止裸色**：颜色一律 `var(--token)`；半透明用 `rgba(var(--X-rgb), α)`。
2. **JS 生成的 DOM**：内联 `style="..."` 同样写 `var(--token)`（浏览器支持）。
   例外：Chart.js / Plotly 配置对象内无法解析 CSS 变量，图表系列色可用字面值，
   但轴/网格/文字色请取 token 的等值色（`#9fb1c9` 等）。
3. **区块局部变量**（`--radar-*`、`--research-*`、`--ai-research-*`、`--rs-*`）已全部
   别名到全局 token，新代码直接用全局 token，不要再扩充局部色板。
4. **模板小字说明**用工具类，不写内联样式：
   `.u-hint`（12px 说明）`.u-note`（11px 弱说明）`.u-footnote`（11px 带上下留白）
   `.u-subnote`（弱化后缀）`.u-empty`（表格空态居中）。
5. **文档化例外（紫色族）**：`#c4b5fd / #a855f7 / #7c3aed / #d8b4fe / #1e1b4b / #3b0764`
   仅用于「叙事确认 / 数据过期」等特殊标记（`.signal-mini-flag.is-stale`、
   `.altcoin-src-narrative-confirm`），是有意保留的第五色相，勿扩散到其他场景。
6. **圆角**：小件 4/6/8px，卡片 10–14px，胶囊 999px；不要新造 7/9/15px 之类的奇数值。
7. **改动 CSS/JS 后**：在 `web/asset_versions.py` 里给对应文件版本号 +1，否则浏览器吃旧缓存。
8. **共享壳层**：`style.css` 文件尾部的 `UI SYSTEM v3` 区块是页面外壳与通用控件的最终契约；页面专属规则只调整内容密度与组件排列，不要重新定义 header、tabs、card、form、button、table 的基础尺寸。

## 4. 共享组件基准

- 卡片：`.card`（`--card-bg` 面 + `--card-border` 边 + h3 蓝绿渐变标记条）。
- 状态徽章：绿=系统正常（positive-deep→positive 渐变）、钢蓝=行情 REST、琥珀=模拟盘。
- 按钮：`.btn-primary` 绿系为主操作，`.btn-danger` 红系为破坏性操作，默认灰蓝为次操作。
- 标题：`header h1` 使用 bright→accent-soft→positive-soft 渐变文字，两个页面（`/`、`/news`）共用。

## 5. 空态（empty-state）模式

数据未加载时不留"黑洞"：
- 空的动态容器（无子元素）用 `:not(:has(*))` 直接 `display:none` 折叠
  （见文件尾"统一补丁"段；注意 `:has()` **不可嵌套** `:has()`，等价写法用
  `:not(:has(父 后代))`）。
- 带占位文案的容器统一渲染为虚线内嵌面板：
  `border: 1px dashed var(--border-subtle); background: var(--panel-bg); color: var(--text-faint)`。
- 区块层大量 `display !important`，空态补丁需同样加 `!important` 才能生效。

## 6. 历史背景

2026-07 做过一次全量归并：150+ 种漂移 hex、560+ 处 rgba、模板与 JS 内联色统一收敛到上述
token（约 2300 处替换），五套区块局部色板重指向全局。此前的漂移原因是各次「区块级重设计」
自带色板追加在文件尾部。后续共享外壳改动统一编辑 `style.css` 尾部的 `UI SYSTEM v3` 区块，禁止新增局部色板。
