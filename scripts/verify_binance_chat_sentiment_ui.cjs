/* Read-only loopback smoke for the BinanceChinese tracker. */
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'C:/Users/zxc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const base=process.env.RESEARCH_UI_BASE_URL||'http://127.0.0.1:8000';
assert(['127.0.0.1','localhost','[::1]'].includes(new URL(base).hostname),'Only a loopback server is allowed.');
const output=path.resolve(process.env.RESEARCH_UI_OUTPUT||'data/research/binance_chat_sentiment_20261031/ui');
async function main(){
  fs.mkdirSync(output,{recursive:true});
  const executable=[process.env.BROWSER_EXECUTABLE,chromium.executablePath(),'C:/Program Files/Google/Chrome/Application/chrome.exe','C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'].find(p=>p&&fs.existsSync(p));
  assert(executable,'A local browser executable is required.');
  const browser=await chromium.launch({headless:true,executablePath:executable});
  const context=await browser.newContext({viewport:{width:1440,height:1100}});
  const page=await context.newPage();
  const errors=[];
  page.on('pageerror',e=>errors.push(e.name));
  try{
    await page.goto(base+'/#strategies',{waitUntil:'domcontentloaded',timeout:60000});
    const response=await context.request.get(base+'/api/ai/research-trackers',{timeout:30000});
    assert.equal(response.status(),200);
    const data=await response.json();
    const row=data.trackers.find(r=>r.id==='telegram_binance_chat_sentiment');
    assert(row?.chat_sentiment,'The updated API must expose sentiment diagnostics.');
    assert.equal(row.chat_sentiment.windows.length,2);
    assert.equal(row.ledgers.length,4);
    const event=row.chat_sentiment.event_time;
    assert.equal(event?.rolling?.length,2,'Versioned event-time observer must be initialized.');
    assert.equal(event.books.length,4,'Separate event-time paper books');
    if(!row.n)assert.equal(row.mean_pct,null);
    // Render the just-checked response; periodic updates remain the ordinary app's job.
    await page.waitForFunction(()=>!!window.ResearchTrackers,{timeout:30000});
    await page.evaluate(d=>window.ResearchTrackers.render(d),data);
    await page.locator('[data-tracker-filter="telegram"]').click();
    await page.locator('#research-tracker-search').fill('币安中文群');
    await page.locator('[data-tracker-toggle="telegram_binance_chat_sentiment"]').click();
    const detail=page.locator('#rt-detail-telegram_binance_chat_sentiment');
    await detail.waitFor({state:'visible'});
    assert.equal(await detail.locator('.rt-sentiment-window').count(),2);
    assert.equal(await detail.locator('.rt-ledger-table tbody tr').count(),4);
    assert.equal(await detail.locator('.rt-event-window').count(),2);
    assert.equal(await detail.locator('.rt-event-books tbody tr').count(),4);
    const text=await detail.innerText();
    assert(text.includes('BTC 群情绪观察')&&text.includes('分数不是涨跌概率'));
    assert(!/undefined|NaN/.test(text));
    assert(text.includes('每位作者只计最新方向')&&text.includes('不计入前向样本'));
    await detail.locator('.rt-event-grid').scrollIntoViewIfNeeded();
    await detail.locator('.rt-event-grid').screenshot({path:path.join(output,'desktop_event_windows.png')});
    const windows=detail.locator('.rt-sentiment-windows');
    await windows.scrollIntoViewIfNeeded();
    await windows.screenshot({path:path.join(output,'desktop_windows.png')});
    const desktopOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2);
    const history=detail.locator('details[data-telegram-history]');
    if(await history.count()){
      await history.locator('summary').click();
      await page.evaluate(d=>window.ResearchTrackers.render(d),data);
      assert(await history.getAttribute('open')!==null,'History must survive refresh.');
    }
    await page.setViewportSize({width:430,height:1000});
    const toggle=page.locator('[data-tracker-toggle="telegram_binance_chat_sentiment"]');
    await toggle.click();
    const narrowBaselineWidth=await page.evaluate(()=>document.documentElement.scrollWidth);
    await toggle.click();
    await detail.locator('.rt-event-grid').scrollIntoViewIfNeeded();
    const eventBounds=await detail.locator('.rt-event-grid').boundingBox();
    assert(eventBounds.x>=0&&eventBounds.x+eventBounds.width<=432,'Narrow event-time cards fit.');
    await detail.locator('.rt-event-grid').screenshot({path:path.join(output,'narrow_event_windows.png')});
    await windows.scrollIntoViewIfNeeded();
    await windows.screenshot({path:path.join(output,'narrow_windows.png')});
    const narrowOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2);
    const narrowExpandedWidth=await page.evaluate(()=>document.documentElement.scrollWidth);
    const bounds=await windows.boundingBox();
    assert.equal(desktopOverflow,false,'Desktop document overflow');
    assert(narrowExpandedWidth<=narrowBaselineWidth,'Expanding sentiment must not widen the document.');
    assert(bounds.x>=0&&bounds.x+bounds.width<=432,'Narrow sentiment cards must fit the viewport.');
    assert.equal(errors.length,0,'Uncaught JavaScript errors');
    const receipt={ok:true,checked_at:new Date().toISOString(),tracker_count:data.trackers.length,
      windows:row.chat_sentiment.windows.map(w=>({hours:w.hours,status:w.status,authors:w.authors,score:w.score})),
      ledgers:row.ledgers.length,desktop_document_overflow:desktopOverflow,narrow_document_overflow:narrowOverflow,
      event_observer:{status:event.status,new_views:event.new_views_in_segment,books:event.books.length,activation_at:event.activation_at},
      narrow_baseline_width:narrowBaselineWidth,narrow_expanded_width:narrowExpandedWidth,narrow_sentiment_bounds:bounds,page_errors:errors};
    fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2));
    process.stdout.write(JSON.stringify(receipt)+'\n');
  }finally{await context.close();await browser.close();}
}
main().catch(e=>{process.stderr.write(e.name+': '+e.message+'\n');process.exitCode=1;});
