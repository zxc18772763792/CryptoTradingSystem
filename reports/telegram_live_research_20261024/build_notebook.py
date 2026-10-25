"""Build the local, aggregate-only audit companion from the initial frozen sample."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

# Conda's delayed BLAS loading on Windows needs its native directory even when
# invoking the correct interpreter directly, without conda activation.
_dll_handles=[]
if os.name=="nt" and (Path(sys.prefix)/"Library/bin").is_dir():
    native=Path(sys.prefix)/"Library/bin"
    _dll_handles.append(os.add_dll_directory(str(native)))
    os.environ["PATH"]=str(native)+os.pathsep+os.environ.get("PATH","")

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from loguru import logger
logger.remove()
from core.research.telegram_forward import digest
try:
    import nbformat as nbf
    from nbclient import NotebookClient
    EXECUTOR="jupyter_nbclient"
except ModuleNotFoundError:
    # Keep the project environment unchanged: Jupyter is not installed here.
    # These standard-Python cells can be executed in order without a kernel.
    import ast
    import base64
    from contextlib import redirect_stdout,redirect_stderr
    import io
    from types import SimpleNamespace
    import uuid

    class Notebook(dict):
        @property
        def metadata(self): return self["metadata"]
        @metadata.setter
        def metadata(self,value): self["metadata"]=value
        @property
        def cells(self): return self["cells"]
        @cells.setter
        def cells(self,value): self["cells"]=value

    def cell(kind,source):
        result={"cell_type":kind,"id":uuid.uuid4().hex[:8],"metadata":{},"source":source}
        if kind=="code": result.update(execution_count=None,outputs=[])
        return result

    def validate(notebook):
        assert notebook["nbformat"]==4 and notebook["nbformat_minor"]==5
        assert len({c["id"] for c in notebook.cells})==len(notebook.cells)
        for value in notebook.cells:
            assert value["cell_type"] in ("markdown","code") and isinstance(value["source"],str)
            if value["cell_type"]=="code": ast.parse(value["source"])

    nbf=SimpleNamespace(v4=SimpleNamespace(
        new_notebook=lambda:Notebook(nbformat=4,nbformat_minor=5,metadata={},cells=[]),
        new_markdown_cell=lambda source:cell("markdown",source),
        new_code_cell=lambda source:cell("code",source)), validate=validate,
        write=lambda nb,path:Path(path).write_text(json.dumps(nb,ensure_ascii=False,indent=1),encoding="utf-8"))

    class NotebookClient:
        def __init__(self,notebook,**kwargs): self.notebook=notebook
        def execute(self):
            namespace={"__name__":"__notebook__"}
            number=0
            for value in self.notebook.cells:
                if value["cell_type"]!="code": continue
                number+=1; value["execution_count"]=number
                syntax=ast.parse(value["source"])
                last=syntax.body.pop() if syntax.body and isinstance(syntax.body[-1],ast.Expr) else None
                stream=io.StringIO()
                with redirect_stdout(stream),redirect_stderr(stream):
                    exec(compile(syntax,"<notebook>","exec"),namespace)
                    result=eval(compile(ast.Expression(last.value),"<notebook>","eval"),namespace) if last else None
                if stream.getvalue(): value["outputs"].append({"output_type":"stream","name":"stdout","text":stream.getvalue()})
                if result is not None:
                    data={"text/plain":repr(result)}
                    if hasattr(result,"to_html"): data["text/html"]=result.to_html(index=False)
                    value["outputs"].append({"output_type":"execute_result","execution_count":number,"metadata":{},"data":data})
                if "hourly_btc_authors.png" in value["source"]:
                    value["outputs"].append({"output_type":"display_data","metadata":{},"data":{
                        "image/png":base64.b64encode((OUT/"hourly_btc_authors.png").read_bytes()).decode()}})
            self.notebook.metadata["execution_note"]="Executed standard Python cells sequentially in project interpreter; Jupyter kernel unavailable."
    EXECUTOR="project_python_sequential_cells_no_jupyter_kernel"

DATA=ROOT/"data/research/telegram_live_20261024"
OUT=Path(__file__).resolve().parent
frozen=json.loads((DATA/"research_features.json").read_text(encoding="utf-8"))
snapshot_path=DATA/"research_metadata.json"
if not snapshot_path.exists():
    db=sqlite3.connect(f"file:{(DATA/'observations.sqlite').as_posix()}?mode=ro",uri=True)
    rows=[json.loads(r[0]) for r in db.execute("SELECT row_json FROM messages WHERE mode='historical' ORDER BY published_at,source_id")]
    ids={r["source_id"] for r in rows}
    labels={key:json.loads(blob) for key,blob in db.execute("SELECT source_id,label_json FROM labels") if key in ids}
    assert digest(labels)==frozen["labels_sha"], "Initial labels changed; do not silently replace the original sample"
    keep=("source_id","channel","published_at","mode","author_hash","sender_is_bot","edited","forwarded","text_sha256")
    projection=[]
    for row in rows:
        item={key:row.get(key) for key in keep}
        value=labels.get(row["source_id"],{})
        item["kind"]=value.get("kind")
        item["events"]=[{key:e[key] for key in ("symbol","side","confidence")} for e in value.get("events",[])]
        projection.append(item)
    snapshot_path.write_text(json.dumps({"rows":projection,"initial_full_labels_sha":frozen["labels_sha"]},ensure_ascii=False),encoding="utf-8")
checksum=hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
nb=nbf.v4.new_notebook()
nb.metadata["kernelspec"]={"name":"python3","display_name":"Python 3","language":"python"}
nb.metadata["executor"]=EXECUTOR
nb.cells=[
nbf.v4.new_markdown_cell("# Telegram 三来源：研究与前向观察审计\n\n## tl;dr\n\n当前没有可验证的交易优势。初始样本中的主群方向观点较稀疏，严格小时规则没有合格信号；CryptoQuant 历史编辑稿不用于还原原始时点，API 群只做技术风险观察。前向账本已独立启动，尚无成熟收益，不能把空样本解释为零收益。"),
nbf.v4.new_markdown_cell("## Context & Methods\n\n决策：是否值得为三个来源继续收集前向证据，暂不注册实盘策略。初始规则在历史价格读取前冻结：BTC、每小时至少5个方向观点账号、多空不平衡绝对值≥0.4、顺势/反向分别观察、持有6小时；CryptoQuant方向观点持有24小时。基于文本密度增加的6小时窗口只用于前向探索，不声称是历史留出验证。\n\n### Key Assumptions\n\n- 用户已确认研究及AI处理授权，具体依据待补充；只输出汇总。\n- 主群触及12,000条采集上限，只覆盖约38小时；其余两个来源按45天边界采集。\n- 发送账号经本地加盐去标识化，账号数不等同于独立真人或分析师数。\n- 模型只接收脱敏文本、不接收价格；Gemini 2.5 Flash另次复核不构成独立人工真值。\n- 编辑过的历史、转发、机器人及不明确资产的内容不作为历史交易信号。前向以首次接收和解析完成为可用时点。\n- 收益计划扣20bp往返成本、40bp压力成本及实际资金费；缺数据保留未结算状态。"),
nbf.v4.new_markdown_cell("## Data\n\n### 1. Verify the frozen aggregate inputs\n\n源记录、协议和行情回执保存在本机忽略目录。这里使用不含正文或身份信息的元数据副本；通过固定哈希防止后续采集改变这次分析。"),
nbf.v4.new_code_cell(f'''from pathlib import Path
import json,hashlib
from collections import Counter,defaultdict
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(r"{ROOT}")
DATA=ROOT/'data/research/telegram_live_20261024'
OUT=ROOT/'reports/telegram_live_research_20261024'
raw=(DATA/'research_metadata.json').read_bytes()
assert hashlib.sha256(raw).hexdigest()=="{checksum}"
rows=json.loads(raw)['rows']
profile=json.loads((DATA/'research_features.json').read_text(encoding='utf-8'))['profile']
pd.DataFrame([{{'source':s,'messages':p['messages'],'text_messages':p['text_messages'],'first':p['first'],'last':p['last'],'edited':p['edited'],'directional':p['directional_messages']}} for s,p in profile.items()])'''),
nbf.v4.new_markdown_cell("## Results\n\n### 2. Recompute BTC author coverage\n\n小时级作者数来自实际可见历史快照中的已复核 BTC 观点；不把缺少明确观点解释为中性情绪。"),
nbf.v4.new_code_cell('''btc=[]
for row in rows:
    if row['channel']!='BinanceChinese' or row['kind']!='directional': continue
    for event in row['events']:
        if event['symbol']=='BTCUSDT' and event['confidence']>=.7:
            btc.append({'at':pd.Timestamp(row['published_at']),'author':row['author_hash'],'side':event['side']})
frame=pd.DataFrame(btc)
frame['hour']=frame['at'].dt.floor('h')
hourly=frame.groupby('hour')['author'].nunique()
assert len(frame)==27 and frame['author'].nunique()==17 and hourly.max()==4
main=[r for r in rows if r['channel']=='BinanceChinese']
first=min(pd.Timestamp(r['published_at']) for r in main).ceil('h')
last=max(pd.Timestamp(r['published_at']) for r in main).floor('h')
index=pd.date_range(first,last-pd.Timedelta(hours=1),freq='h')
visible=hourly.reindex(index,fill_value=0)
diagnostics={'btc_views':len(frame),'btc_authors':int(frame['author'].nunique()),'maximum_hourly_authors':int(hourly.max()),'complete_visible_hours':len(index),'minimum_required_authors':5,'qualified_original_features':0}
(OUT/'diagnostics.json').write_text(json.dumps(diagnostics,indent=2),encoding='utf-8')
diagnostics'''),
nbf.v4.new_markdown_cell("### 3. Coverage against the frozen hourly rule"),
nbf.v4.new_code_cell('''plt.rcParams.update({'font.size':11,'axes.spines.top':False,'axes.spines.right':False})
fig,ax=plt.subplots(figsize=(10,3.8),layout='constrained')
ax.bar(visible.index,visible.values,width=.032,color='#286a9e',label='BTC directional accounts')
ax.axhline(5,color='#a94d20',linestyle='--',label='Frozen minimum: 5 accounts')
ax.set_ylim(0,6); ax.set_yticks(range(7)); ax.set_ylabel('Distinct sending accounts')
ax.set_title('Visible BTC viewpoints do not reach the hourly author minimum')
ax.set_xlabel('UTC hours; partial first/last hours excluded')
ax.legend(frameon=False,loc='upper left')
import matplotlib.dates as mdates
ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
fig.savefig(OUT/'hourly_btc_authors.png',dpi=160)
plt.show()'''),
nbf.v4.new_markdown_cell("### 4. Confirm absence of a return sample\n\n零合格信号不等于收益为零。原规则没有交易样本，因此没有胜率、平均收益或显著性结论。"),
nbf.v4.new_code_cell('''result=json.loads((DATA/'research_results.json').read_text(encoding='utf-8'))
assert result['feature_count']==0 and result['events']==[]
summary=json.loads((DATA/'forward_summary.json').read_text(encoding='utf-8'))
pd.DataFrame([{'paper_book':key,'decisions_at_snapshot':v['decisions'],'mature_at_snapshot':v['mature_settled'],'mean_net':v['mean_net_per_notional'],'promotion_allowed':v['promotion_allowed']} for key,v in summary['books'].items()])'''),
nbf.v4.new_markdown_cell("## Takeaways\n\n- 中文主群有可识别的 BTC 观点，但当前窗口不足以验证收益；保持原规则，另行前向观察6小时聚合。\n- CryptoQuant 采用首次实际接收版本做未来观察，避免把当前编辑稿回填为当时已知文本。\n- API群关注技术故障压力，不将报错词当作看空交易。\n- 五个账本分别冻结、分别评估，不混成账户收益；原规则和探索规则不择优删去失败者。\n- 观察达到100笔成熟样本及8个日历周后才进行进一步人工研究复核，仍需处理多重比较、独立留出和真实成交假设；任何筛查结果均不自动放行实盘。\n\n交接入口：`docs/TELEGRAM_AGENT_HANDOFF.md`。")]
nbf.validate(nb)
NotebookClient(nb,timeout=180,kernel_name='python3',resources={'metadata':{'path':str(ROOT)}}).execute()
nbf.write(nb,OUT/'audit.ipynb')
print(json.dumps({'notebook':str(OUT/'audit.ipynb'),'metadata_sha256':checksum,'cells':len(nb.cells),'executed':True,'executor':EXECUTOR}))
