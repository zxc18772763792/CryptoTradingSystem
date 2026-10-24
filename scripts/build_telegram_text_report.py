"""Build evidence and execute an offline audit notebook.

Run inside the project's activated conda environment (or ``conda run -p ...``)
so Matplotlib resolves its native Windows DLLs from that environment.
"""
from __future__ import annotations
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.telegram_text_signals import write_json

DATA = ROOT / 'data/research/cryptoquant_text_20261023'
OUT = ROOT / 'reports/telegram_text_20261023'


def pct(value):
    return '--' if value is None else f'{value * 100:+.2f}%'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    result = json.loads((DATA / 'results.json').read_text(encoding='utf-8'))
    manifest = json.loads((DATA / 'archive/collection_manifest.json').read_text(encoding='utf-8'))
    events = [json.loads(line) for line in (DATA / 'event_outcomes.jsonl').read_text(encoding='utf-8').splitlines()]
    quality = result['quality']
    rows = [r for r in result['results'] if r['phase'] == 'holdout' and r['symbol'] == 'BTCUSDT']
    primary = [r for r in rows if r['hours'] == 24]
    table = ['| 标签集 | 持有 | 样本 | 平均净收益/笔 | 胜率 | 周聚类95%区间 | 随机时点q值 |',
             '|---|---:|---:|---:|---:|---|---:|']
    for row in rows:
        f = row['funded']
        ci = '不足8个日历周，不报告' if f['ci95'] is None else f"[{pct(f['ci95'][0])}, {pct(f['ci95'][1])}]"
        table.append(f"| {'原始自动分类' if row['signal_set']=='raw' else '语义复核'} | {row['hours']}h | {f['n']} | {pct(f['mean'])} | {f['win_rate']:.1%} | {ci} | {row['random_time'].get('q_fdr', float('nan')):.3f} |")
    long_events = [r for r in events if r['signal_set'] == 'semantic_reviewed' and r['phase'] == 'holdout' and r['symbol'] == 'BTCUSDT' and r['hours'] == 720 and r.get('net') is not None]
    long_excess = sum(r['excess_long'] for r in long_events)
    best_excess = max((r['excess_long'] for r in long_events), default=0)
    concentration = best_excess / long_excess if long_excess else None
    lines = [
        '# Telegram 文本信号研究：CryptoQuant 与 Binance 中文群', '',
        'CryptoQuant 文字具备可提取的方向信息，但本轮没有通过稳定交易信号的证据门槛。两个 Binance 中文群尚未取得群聊历史，不能计算其真实情绪或收益。', '',
        '## 主要结果', '',
        '- BTC 24h 原始自动分类：41 笔，平均净收益 +0.36%/笔，胜率 58.5%；95%区间 [-0.10%, +0.90%]。',
        '- 更严格语义复核：28 笔，平均净收益 +0.60%/笔，胜率 67.9%；95%区间 [-0.02%, +1.24%]，多重检验 q=0.488。',
        '- 上述均为单笔入场名义本金的收益，已扣往返20bp成本及资金费，不是账户收益、复利收益或年化收益。',
        '- 2026 留出期两个半段的24h均值为正，但此前开发期为负：原始168笔 -0.38%/笔，复核112笔 -0.56%/笔，跨时期不稳定。',
        '- 语义复核的30天样本只有6笔；相对同入场时点固定做多的全部净增益来自一次做空。去掉该次后超额均值为零；不能将6/6解释为可靠的100%胜率。原始分类的同周期结果为7笔、平均 -6.99%/笔，显示长持有期对文本过滤及首笔非重叠入场非常敏感。',
        '- ETH 的留出样本仅4–7笔，BNB没有留出交易，均不能下有效性结论。', '',
        '## 数据与时间口径', '',
        f"- 来源仅为 [cryptoquant_official](https://t.me/s/cryptoquant_official)，不是 cryptoquant_alert 自动告警。归档{manifest['page_count']}个公开预览页，{quality['input_rows']}条文本，覆盖2021-05-19至2026-10-23；达到页数上限，未声称完整频道历史。",
        f"- 排除编辑标记{quality['edited_excluded']}条、重复文本{quality['duplicate_text']}条，保留{quality['usable']}条。另有{len(manifest['unparsed_messages'])}条可见但未解析的消息，未做图片OCR或视频解读。",
        '- 先冻结文本标签和语义复核，再载入行情。开发期截至2026年前并留出7天隔离；留出期自2026-01-01起。',
        '- 本地 Binance USDT 永续小时K线覆盖至2026-10-20 00:00 UTC；需要此后价格或尚未成熟的事件排除。输入文件哈希及副本均已保存。',
        '- 文本至少明确提及BTC/ETH/BNB之一；仅无条件当前方向观点、模型置信度≥0.7入选。每币每源只持有一笔，重叠信号不叠加。',
        '- 入场为发布后至少60秒的下一小时开盘，固定1/6/24/72/168小时退出；30天周期在首次价格对齐前补充登记。无事后止盈止损调参。',
        '- 往返20bp（含手续费及滑点假设），压力40bp；资金费按结算标价相对入场价折算，约定入场时刻之后、退出时刻及之前结算。缺失资金费不当成零。',
        '- 同时检查原始标签及严格语义复核，保留全部结果；复核由同一个模型另次调用完成，并不构成独立人工真值。', '',
        '## 2026年BTC留出结果', '', *table, '',
        '![留出期表现及长持有期集中度](results.png)', '',
        '## 对照与统计', '',
        '- 对照包括相同入场时刻固定做多、同入场反向交易、同币同方向且相同UTC小时的附近随机时点。',
        '- 随机时点取±28日，排除与事件相隔不超过持有时长的日期；30天持有用±90日。所有随机对照仍须价格及资金费完整，且在同一开发/留出区间。',
        f"- 5,000次随机时点抽样；2,000次按日历周聚类bootstrap；对{result['fdr_family_size']}个有效留出检验进行BH FDR校正。抽样种子固定，少于8周不报告周聚类区间。",
        '- 24h复核样本低于事前30笔门槛，净收益区间及相对固定做多超额区间均跨零，随机时点校正后也未通过，故不接入执行或注册为策略。', '',
        '## Binance 中文群：等待历史文件', '',
        '- 已核验[官方中文群](https://t.me/BinanceChinese)与[API中文群](https://t.me/Binance_api_Chinese)的公开简介；公开预览未提供消息历史。本次工具不能读取桌面版Telegram中的消息。',
        '- 已准备JSON导入、凭据及联系方式脱敏、UTC时间校验、作者去标识化、去重及文本分类。API报错、代码、签名和订单参数讨论不能算看空/恐慌。',
        '- 情绪特征按已结束的UTC小时聚合，每位作者每币每小时等权；至少5位有方向观点的作者、绝对多空不平衡度≥0.4才形成待测候选。两个群分别计算，不能与研究频道或彼此混成同一情绪分数。该特征提取器已测试，但尚无真实群数据验证。',
        '- 请从每个群右上角菜单分别导出聊天记录为JSON，尽量至少6个月，关闭媒体。Telegram官方支持[单独聊天导出](https://telegram.org/blog/export-and-more)。确认导出的是群内可见历史，而非仅自己的发言。',
        '- 文件可放到 `data/research/telegram_exports/BinanceChinese/` 和 `data/research/telegram_exports/Binance_api_Chinese/`；收到路径后才能继续真实情绪回测。不要提供整个账号资料目录。', '',
        '## 边界与审计', '',
        '- 公开预览是采集时可见文本，编辑前版本、删除帖、最初发布时间的可见性均无法还原；即使无编辑标记，也不保证文字从未改动。消息ID缺口原因未知。',
        '- 模型没有收到后续行情及时间戳，但预训练记忆造成的历史知识影响无法完全排除；结果属于回溯探索，不能冒充前向验证。',
        '- 16条按开发/留出及方向分层抽查暴露了历史类比、描述性指标和混合观点的噪音，因此在首次价格对齐前登记了第二轮语义敏感性检验。没有根据收益手改标签。',
        '- 两条首轮格式/证据问题通过离线语义复核解决，记录在 manual_label_resolutions.json；所有原始模型尝试及保守拒绝记录保留。',
        '- 未发送Telegram消息、改动群成员关系、注册策略或提交订单。27项相关测试通过。', '',
        '## 可复跑文件', '',
        '- `data/research/cryptoquant_text_20261023/protocol.json`：原规则与冻结时间。',
        '- `review_protocol.json`、`frozen_labels.jsonl`、`semantic_review/`：二次规则、原始标签及复核。',
        '- `results.json`、`event_outcomes.jsonl`、`market_snapshot/`：汇总、逐笔、价格副本。',
        '- `scripts/research_telegram_text.py backtest --market-root data/research/cryptoquant_text_20261023/market_snapshot`：使用项目解释器，可完全离线重跑行情部分。',
        '- [审计笔记本](audit.ipynb)：已执行；校验输入哈希并从快照重新计算两个24h结果。',
    ]
    (OUT / 'REPORT.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    for name in ['BinanceChinese', 'Binance_api_Chinese']:
        (ROOT / 'data/research/telegram_exports' / name).mkdir(parents=True, exist_ok=True)

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.7), gridspec_kw={'width_ratios': [1.2, 1]})
    horizons = [24, 72, 168]
    for selection, color, offset, label in [('raw', '#3aa6ff', -.12, 'Initial labels'), ('semantic_reviewed', '#168f68', .12, 'Semantic reviewed')]:
        values = [next(r for r in rows if r['signal_set'] == selection and r['hours'] == h) for h in horizons]
        means = np.array([r['funded']['mean'] * 100 for r in values])
        lo = means - np.array([r['funded']['ci95'][0] * 100 for r in values])
        hi = np.array([r['funded']['ci95'][1] * 100 for r in values]) - means
        axes[0].errorbar(np.arange(3) + offset, means, yerr=[lo, hi], fmt='o', capsize=5, color=color, label=label)
    axes[0].axhline(0, color='#607080', linewidth=.8)
    axes[0].set(xticks=np.arange(3), xticklabels=['24h', '72h', '7d'], ylabel='Mean net return / trade (%)', title='BTC 2026 holdout: 95% week-cluster intervals')
    axes[0].legend(frameon=False, fontsize=9)
    excess = [r['excess_long'] * 100 for r in long_events]
    axes[1].bar(range(len(excess)), excess, color=['#168f68' if v > 0 else '#8192a3' for v in excess])
    axes[1].set(xticks=range(len(excess)), xticklabels=[r['entry_at'][:10] for r in long_events],
                ylabel='Excess vs same-entry long (percentage points)', title='30d reviewed: excess concentrated in one trade')
    axes[1].tick_params(axis='x', labelrotation=45, labelsize=8)
    for ax in axes:
        ax.spines[['top', 'right']].set_visible(False)
        ax.grid(axis='y', alpha=.15)
    fig.tight_layout()
    fig.savefig(OUT / 'results.png', dpi=160)
    plt.close(fig)

    # Execute the exact notebook cells with the project interpreter, offline.
    cells = [
        "from pathlib import Path\nimport json, sys, hashlib\nimport numpy as np\nROOT = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / 'AGENTS.md').exists())\nsys.path.insert(0, str(ROOT))\nDATA = ROOT / 'data/research/cryptoquant_text_20261023'\nresult = json.loads((DATA / 'results.json').read_text(encoding='utf-8'))\nprint(result['quality'])",
        "for relative, meta in result['market_sources'].items():\n    snapshot = DATA / 'market_snapshot' / relative\n    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == meta['sha256']\nassert hashlib.sha256((DATA / 'frozen_labels.jsonl').read_bytes()).hexdigest() == result['quality']['labels_sha256']\nprint('All text-label and market-snapshot hashes match')",
        "from scripts.research_telegram_text import Market, independent_events\nfrom core.research.telegram_text_signals import sha\nlabels = [json.loads(line) for line in (DATA / 'frozen_labels.jsonl').read_text(encoding='utf-8').splitlines()]\nreviewed = []\nfor label in labels:\n    events = []\n    for event in label['events']:\n        if event['confidence'] < .7:\n            continue\n        review = json.loads((DATA / 'semantic_review' / (sha(label['source_id'] + '|' + event['symbol']) + '.json')).read_text(encoding='utf-8'))\n        if review['accept']:\n            events.append(event)\n    reviewed.append({**label, 'events': events})\nmarket = Market(DATA / 'market_snapshot')\nfor selection, inputs in [('raw', labels), ('semantic_reviewed', reviewed)]:\n    entries, skipped = independent_events(inputs, 'cryptoquant_official', 'BTCUSDT', 24, 'holdout')\n    net = [market.outcome('BTCUSDT', r['entry_at'], 24, r['side']).get('net') for r in entries]\n    net = [v for v in net if v is not None]\n    recorded = next(r for r in result['results'] if r['signal_set'] == selection and r['symbol'] == 'BTCUSDT' and r['phase'] == 'holdout' and r['hours'] == 24)['funded']\n    assert len(net) == recorded['n']\n    assert abs(np.mean(net) - recorded['mean']) < 1e-12\n    print(selection, len(net), 'mean net', round(np.mean(net)*100, 4), '%')",
    ]
    namespace, notebook_cells = {}, []
    for i, code in enumerate(cells, 1):
        capture = io.StringIO()
        with contextlib.redirect_stdout(capture):
            exec(compile(code, f'audit-cell-{i}', 'exec'), namespace)
        notebook_cells.append({'cell_type': 'code', 'execution_count': i, 'metadata': {}, 'source': code.splitlines(True),
                               'outputs': [{'output_type': 'stream', 'name': 'stdout', 'text': capture.getvalue().splitlines(True)}]})
    write_json(OUT / 'audit.ipynb', {'nbformat': 4, 'nbformat_minor': 5, 'metadata': {'kernelspec': {'name': 'python3', 'display_name': 'crypto_trading', 'language': 'python'}}, 'cells': notebook_cells})

    receipt_rows = []
    for r in primary:
        f = r['funded']
        receipt_rows.append({'labels': r['signal_set'], 'trades': f['n'], 'mean_net_pct': f['mean'] * 100,
                             'win_rate_pct': f['win_rate'] * 100, 'ci95_low_pct': f['ci95'][0] * 100,
                             'ci95_high_pct': f['ci95'][1] * 100, 'q_fdr': r['random_time']['q_fdr']})
    sources = {'schemaVersion': 1, 'items': [{
        'id': 'cryptoquant-btc-holdout', 'title': 'CryptoQuant BTC：2026年24小时留出回测',
        'queries': [{'id': 'btc-24h', 'source': {
            'label': 'CryptoQuant公开文本与Binance永续行情快照',
            'files': [{'label': 'frozen_labels.jsonl'}, {'label': 'results.json'}, {'label': 'market_snapshot'}],
            'metricDefinitions': [{'label': '平均净收益/笔', 'definition': '按单笔入场名义本金计，扣往返20bp及资金费；不是账户或年化收益。'}],
            'filters': ['BTCUSDT', '24小时持有', '每币同源不叠仓', '明确币种且方向置信度≥0.7', '排除编辑与重复文本'],
            'caveats': ['公开预览不是当时可知版本，删除及历史修订无法复原。', '语义复核属于同模型二次调用的敏感性检验，未构成人工真值。', '两个置信区间均跨零；语义复核只有28笔，未达30笔门槛。'],
            'evidenceFlow': [{'kind': 'validation', 'title': '快照复算', 'detail': '审计笔记本核对文本标签及行情文件哈希，并从价格副本复算41笔与28笔的平均净收益，均与汇总一致。'},
                             {'kind': 'method', 'title': '统计对照', 'detail': '5,000次附近同小时随机时点抽样，2,000次日历周聚类bootstrap；24个留出检验做BH FDR。'}]},
            'reportingPeriod': '留出期从2026-01-01起；价格截点2026-10-20 00:00 UTC',
            'columns': list(receipt_rows[0]), 'rows': receipt_rows,
            'methods': [{'language': 'python', 'code': 'net = side * (exit_open / entry_open - 1) - 0.002 + funding_return\nmean_net_pct = 100 * np.mean(net_returns)'}]}]}, {
        'id': 'telegram-source-coverage', 'title': '文本覆盖与可用性',
        'queries': [{'id': 'archive-quality', 'source': {
            'label': 'CryptoQuant归档清单及质量检查', 'files': [{'label': 'collection_manifest.json'}, {'label': 'quality.json'}],
            'metricDefinitions': [{'label': '保留文本', 'definition': '公开归档文本排除带编辑标记及重复内容后的消息数；不代表交易信号数。'}],
            'caveats': ['160页达到本次采集上限，未覆盖完整频道历史。', '两个Binance中文群尚无消息历史，不能估计其情绪或收益。']},
            'reportingPeriod': '2021-05-19至2026-10-23（UTC）',
            'columns': ['source', 'text_rows', 'edited_excluded', 'duplicate_excluded', 'retained'],
            'rows': [{'source': 'cryptoquant_official', 'text_rows': quality['input_rows'], 'edited_excluded': quality['edited_excluded'], 'duplicate_excluded': quality['duplicate_text'], 'retained': quality['usable']}],
            'methods': [{'language': 'calculation', 'code': '2517 - 156 - 22 = 2339'}]}]}]}
    write_json(OUT / 'sources.json', sources)
    write_json(OUT / 'verification.json', {'related_tests_passed': 27, 'notebook_executed': True, 'primary_means_recomputed': True,
               'market_hashes_verified': len(result['market_sources']), 'long_hold_excess_concentration': concentration,
               'no_chat_history_obtained': ['BinanceChinese', 'Binance_api_Chinese']})
    print(json.dumps({'report': str(OUT / 'REPORT.md'), 'audit_notebook_executed': True, 'concentration': concentration}))


if __name__ == '__main__':
    main()
