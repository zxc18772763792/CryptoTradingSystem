"""Standalone research figure from the inspected paired-horizon result table."""
import json
from pathlib import Path


def main():
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    root=Path(__file__).resolve().parents[1]
    cells=json.loads((root/'data/research/alastar_horizons_20261022/summary.json').read_bytes())['cells']
    groups=[('text_fresh','Fresh text calls'),('text_broad','Broad text views'),('image_short','Image first leg down')]
    fig=make_subplots(rows=1,cols=3,subplot_titles=[label for _,label in groups],horizontal_spacing=.08)
    for col,(cohort,label) in enumerate(groups,1):
        rows=sorted([r for r in cells if r['mode']=='hour_common' and r['phase']=='all' and r['cohort']==cohort],key=lambda r:r['hours'])
        x=list(range(len(rows))); y=[r['paired_excess_gain_vs_1h']*100 for r in rows]
        low=[(r['paired_excess_gain_vs_1h']-r['paired_gain_ci95'][0])*100 for r in rows]
        high=[(r['paired_gain_ci95'][1]-r['paired_excess_gain_vs_1h'])*100 for r in rows]
        fig.add_trace(go.Scatter(x=x,y=y,mode='markers',marker={'color':'#2962a3','size':9},
                                error_y={'type':'data','symmetric':False,'array':high,'arrayminus':low,'thickness':1.5,'width':5},
                                name=f'{label}: common n={rows[0]["n"]}',showlegend=False,
                                hovertemplate='Holding index %{x}<br>Gain %{y:.2f} pp<extra></extra>'),row=1,col=col)
        fig.add_hline(y=0,line_color='#9ca3af',line_width=1,row=1,col=col)
        fig.update_xaxes(tickmode='array',tickvals=x,ticktext=['1h','4h','24h','72h','168h'],title_text=f'Holding period; common n={rows[0]["n"]}',row=1,col=col)
        fig.update_yaxes(title_text='Excess gain vs1h (pp)',row=1,col=col)
    fig.update_layout(template='plotly_white',width=1500,height=560,font={'family':'Arial','size':13},
                      title={'text':'Matched horizon comparison — weekly bootstrap 95% descriptive intervals<br><sup>Same signal, entry and pre-entry null starts; historical exploration, no proven winner</sup>','x':.5},margin={'t':100,'b':75,'l':65,'r':40})
    path=root/'reports/alastar_horizons_20261022/paired_horizon_intervals.html'
    fig.write_html(path,include_plotlyjs=True,full_html=True); print(path)


if __name__=='__main__': main()
