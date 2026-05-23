#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fix mojibake in ai_research.js caused by UTF-8 bytes being misread as GBK."""
import sys
import io
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Order: longer strings first to avoid partial replacement
MOJI_MAP = [
    # Long phrases first
    ('ML缁勪欢鏈縺娲伙紙闇€璁粌妯″瀷锛夛紝淇″彿浠呯敤 LLM+Factor', 'ML组件未激活（需训练模型），信号仅用 LLM+Factor'),
    ('OHLCV + 鏂伴椈 + 瀹忚', 'OHLCV + 新闻 + 宏观'),
    ('OHLCV + 鏂伴椈', 'OHLCV + 新闻'),
    ('OHLCV + 瀹忚', 'OHLCV + 宏观'),
    ('浠?OHLCV', '仅OHLCV'),
    ('瀹炵洏鍐崇瓥閰嶇疆淇濆瓨澶辫触', '实盘决策配置保存失败'),
    ('鐮旂┒鐩爣澶煭锛堣嚦灏?涓瓧绗︼級', '研究目标太短（至少8个字符）'),
    ('鍔犺浇璇︽儏澶辫触', '加载详情失败'),
    ('鎵撳紑娉ㄥ唽澶辫触', '打开注册失败'),
    ('鏆傛棤鐮旂┒浠诲姟', '暂无研究任务'),
    ('鍗曟杩愯瀹屾垚', '单次运行完成'),
    ('鏈€鍚庤繍琛?', '最后运行'),
    ('鏈€鍚庤繍琛', '最后运行'),
    ('浼犵粺瑙勫垯', '传统规则'),
    ('鍒嗘暟瀵瑰簲棰滆壊绛夌骇', '分数对应颜色等级'),
    ('姝ｅ湪閲囬泦甯傚満涓婁笅鏂?', '正在采集市场上下文'),
    ('鐮旂┒鐩爣澶煭', '研究目标太短'),
    ('鏆傛棤鍊欓€夌瓥鐣', '暂无候选策略'),
    ('鏆傛棤鏁版嵁', '暂无数据'),
    ('鍙栨秷杩愯', '取消运行'),
    ('杩愯鐮旂┒', '运行研究'),
    ('涓嶅彲杩愯', '不可运行'),
    ('淇濆瓨涓?...', '保存中...'),
    ('淇濆瓨涓?..', '保存中..'),
    ('淇濆瓨涓', '保存中'),
    ('璇锋眰澶辫触', '请求失败'),
    ('鎺ュ彛瓒呮椂', '接口超时'),
    ('淇″彿澶辫触', '信号失败'),
    ('淇″彿鍒嗚В', '信号分解'),
    ('淇″彿寰界珷', '信号徽章'),
    ('鍋滄澶辫触', '停止失败'),
    ('鍚姩澶辫触', '启动失败'),
    ('杩愯澶辫触', '运行失败'),
    ('鎵瑰噯澶辫触', '批准失败'),
    ('鎷掔粷澶辫触', '拒绝失败'),
    ('鍔犺浇澶辫触', '加载失败'),
    ('杈呭姪澶辫触', '辅助失败'),
    ('鏂伴椈浜嬩欢', '新闻事件'),
    ('鏈熸潈鍋忔枩', '期权偏斜'),
    ('璁㈠崟棰勮澶辫触', '订单预览失败'),
    ('鐢熸垚璁㈠崟棰勮', '生成订单预览'),
    ('宸叉嫆缁', '已拒绝'),
    ('宸叉彁浜?', '已提交'),
    ('宸叉彁浜', '已提交'),
    ('鍗囩骇涓哄疄鐩樺', '升级为实盘候选'),
    ('浜哄伐瀹℃壒', '人工审批'),
    ('瀵规瘮閫変腑', '对比选中'),
    ('鍒锋柊涓?...', '刷新中...'),
    ('鍒锋柊涓?..', '刷新中..'),
    ('鍒锋柊浜?', '刷新于'),
    ('鍒锋柊浜', '刷新于'),
    ('鍒锋柊涓', '刷新中'),
    ('鍒嗘瀽涓?..', '分析中..'),
    ('鍒嗘瀽涓', '分析中'),
    ('鍋滄涓?...', '停止中...'),
    ('鍋滄涓?..', '停止中..'),
    ('鍋滄涓', '停止中'),
    ('杩愯涓?...', '运行中...'),
    ('杩愯涓?..', '运行中..'),
    ('鍗曟杩愯', '单次运行'),
    ('杩愯娆℃暟', '运行次数'),
    ('ML椹卞姩', 'ML驱动'),
    ('GLM/AI椹卞姩', 'GLM/AI驱动'),
    ('AI 杈呭姪', 'AI 辅助'),
    ('AI 寤鸿', 'AI 建议'),
    ('椹卞姩', '驱动'),
    ('瀹忚', '宏观'),
    ('鏂伴椈', '新闻'),
    ('绾哥洏', '纸盘'),
    ('椋庢帶', '风控'),
    ('瀹℃壒', '审批'),
    ('淇″彿', '信号'),
    ('淇濆瓨', '保存'),
    ('鏂瑰悜', '方向'),
    ('鍋滄', '停止'),
    ('鍗曟', '单次'),
    ('妯″紡', '模式'),
    ('杩愯', '运行'),
    ('鎵瑰噯', '批准'),
    ('鎷掔粷', '拒绝'),
    ('鐪嬪', '看多'),
    ('鐪嬬┖', '看空'),
    ('鎸佸钩', '持平'),
    ('鍚堣', '综合'),
    ('鏍囩殑', '标的'),
    ('姝㈡崯', '止损'),
    ('姝㈢泩', '止盈'),
    ('澶忔櫘', '夏普'),
    ('鑳滅巼', '胜率'),
    ('鍒嗘暟', '分数'),
    ('鍒嗚В', '分解'),
    ('鍥炴挙', '回撤'),
    ('鐩稿叧', '相关'),
    ('鍔犺浇涓', '加载中'),
    ('鍔犺浇', '加载'),
    ('缃俊搴?', '置信度 '),
    ('缃俊搴', '置信度'),
    ('寤鸿浠撲綅', '建议仓位'),
    ('杈呭姪', '辅助'),
    ('鍒犻櫎', '删除'),
    ('鍙栨秷', '取消'),
    ('澶辫触', '失败'),
    ('鍒濆', '初始'),
    ('寤鸿', '建议'),
    ('绛栫暐', '策略'),
    ('鐢熸垚', '生成'),
    ('瀹炵洏', '实盘'),
    ('鏆傛棤', '暂无'),
    ('鐮旂┒', '研究'),
    ('鍐崇瓥閰嶇疆', '决策配置'),
    ('妯″瀷', '模型'),
    ('鏈縺娲', '未激活'),
    ('宸插惎鐢?', '已启用'),
    ('宸插惎鐢', '已启用'),
    ('宸查', '已查'),
    ('璁板綍', '记录'),
    ('浜哄伐', '人工'),
    ('鍔犱腑', '加载中'),
    ('閫€褰?', '退役'),
    ('閫€褰', '退役'),
    ('閫€', '退'),
    ('寰呭', '待审批'),
    ('鍚疄鐩?', '开实盘'),
    ('鍚疄鐩', '开实盘'),
    ('浠呯焊鐩?', '仅纸盘'),
    ('浠呯焊鐩', '仅纸盘'),
    # filter category
    ('瓒嬪娍', '趋势'),
    ('闇囪崱', '震荡'),
    # separator dot (·)
    # Note: '路' is sometimes used correctly as 路 (road) in other contexts
    # Only replace when used as separator (surrounded by spaces or numbers)
    # We'll handle this carefully with regex below
    # box-drawing separator chars
    ('鈹€鈹€', '──'),
    ('鈺愨晲', '━'),
    # other fragments
    ('澶忔', '夏'),
    ('鍗婃湡', '半期'),
    ('浜哄伐', '人工'),
    ('鏆撮湶', '暴露'),
    ('鍏煎', '兼容'),
    ('鍒嗗尯', '分区'),
]

# ??? placeholder fixes (context-specific)
PLACEHOLDER_FIXES = [
    # Agent notifications (must match exactly)
    ("notify('AI???????')", "notify('AI代理已启动')"),
    ('notify("AI???????")', 'notify("AI代理已启动")'),
    ("notify('???????')", "notify('研究提案已生成')"),
    # Live signals panel
    ("'???????'", "'暂无运行中候选'"),
    ('- ????</span>', '- 信号错误</span>'),
    # direction icons
    ("d === 'LONG' ? '?' : d === 'SHORT' ? '?' : '?'", "d === 'LONG' ? '▲' : d === 'SHORT' ? '▼' : '─'"),
    # dot title
    ("running ? '???' : '???'", "running ? '运行中' : '已停止'"),
    # last error label
    ("'??: ${status.last_error}'", "'错误: ${status.last_error}'"),
    # missing run time
    ("status.last_run_at ? status.last_run_at.slice(0, 19) : '??'",
     "status.last_run_at ? status.last_run_at.slice(0, 19) : '--'"),
    # AI context
    ("notify('??????????????'", "notify('研究目标太短（至少8个字符）'"),
    ("notify('?????????'", "notify('研究目标太短（至少8个字符）'"),
    ("btn.textContent = 'AI ?????'", "btn.textContent = 'AI 分析完成'"),
    ("notify('AI ?????????????????????')", "notify('AI 辅助上下文已生成，下次生成提案时将自动使用')"),
    # order preview
    ("'? ?????'", "'⚠ 风控拦截：'"),
    ("'? ??????'", "'⚠ 置信度不足（'"),
    ("'????????'", "'），建议人工确认'"),
    # generateProposal
    ("notifMsg = '???????'", "notifMsg = '研究提案已生成'"),
    # ML offline badge
    ("'${mlOffline ? '?' : ''}'", "'${mlOffline ? '↓' : ''}'"),
    # agent stop notify
    ("notify('AI???????')", "notify('AI代理已停止')"),
]

def fix_separator_dot(content):
    """Replace '路' as separator dot (·) only in specific UI contexts."""
    # In format strings like: '方向 ${dir} ${conf}% 路 Funding'
    # The separator pattern: space + 路 + space (where surrounded by UI content)
    # Only replace in the _collectLiveMarketContext / generateProposal area
    content = content.replace(' 路 Funding ', ' · Funding ')
    content = content.replace(' 路 OFI ', ' · OFI ')
    content = content.replace(' 路 OI ', ' · OI ')
    content = content.replace(' 路 期权偏斜', ' · 期权偏斜')
    content = content.replace(' 路 新闻事件', ' · 新闻事件')
    content = content.replace(' 路 鲸鱼 ', ' · 鲸鱼 ')
    content = content.replace(' 路 新闻 ', ' · 新闻 ')
    # planner notes separator
    content = content.replace("join(' 路 ')", "join(' · ')")
    return content

with open('web/static/js/ai_research.js', 'r', encoding='utf-8') as f:
    content = f.read()

original_len = len(content)
print(f'Original length: {original_len} chars, {len(content.splitlines())} lines')

# Apply mojibake replacements
replaced_count = 0
for moji, correct in MOJI_MAP:
    n = content.count(moji)
    if n > 0:
        content = content.replace(moji, correct)
        replaced_count += n
        print(f'  Replaced {n}x: {repr(moji)} -> {repr(correct)}')

# Apply separator dot fixes
content = fix_separator_dot(content)

# Apply placeholder fixes
for bad, good in PLACEHOLDER_FIXES:
    if bad in content:
        content = content.replace(bad, good)
        print(f'  Placeholder fix: {repr(bad[:50])}')

# Fix the AI error notify that has special chars
old_ai_err = "notify(`AI ????\\n${result?.error || 'LLM ???????'}`"
new_ai_err = "notify(`AI 错误: ${result?.error || 'LLM 服务不可用'}`"
if old_ai_err in content:
    content = content.replace(old_ai_err, new_ai_err)

# Also fix the inline pattern
for old, new in [
    ("notify(`AI ????: ${result?.error || 'LLM ???????'}`,", "notify(`AI 错误: ${result?.error || 'LLM 服务不可用'}`,"),
    ("result?.error || 'LLM ???????'", "result?.error || 'LLM 服务不可用'"),
]:
    if old in content:
        content = content.replace(old, new)
        print(f'  Fixed AI error: {repr(old[:50])}')

with open('web/static/js/ai_research.js', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'\nDone. New length: {len(content)} chars, {len(content.splitlines())} lines')
print(f'Total replacements: {replaced_count}')
