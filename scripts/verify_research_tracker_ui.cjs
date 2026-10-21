/* Isolated headless smoke of the local paper research pages; no trading mutations. */
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base=process.env.RESEARCH_UI_BASE_URL || 'http://127.0.0.1:8000';
assert(['127.0.0.1','localhost','[::1]'].includes(new URL(base).hostname),'Only a loopback test server is allowed.');
const output=path.resolve(process.env.RESEARCH_UI_OUTPUT || 'data/research/review_20261020/ui_verification');
async function main(){
  fs.mkdirSync(output,{recursive:true});
  const browser=await chromium.launch({headless:true,...(process.env.BROWSER_EXECUTABLE?{executablePath:process.env.BROWSER_EXECUTABLE}:{})});
  const context=await browser.newContext({viewport:{width:1440,height:1000}});
  const page=await context.newPage();
  const pageErrors=[];
  page.on('pageerror',e=>pageErrors.push(e.name));
  try{
    await page.goto(base+'/#strategies',{waitUntil:'domcontentloaded',timeout:60000});
    // The normal loopback page load supplies the app's HttpOnly session cookie.
    // No credentials are read, printed, copied into JavaScript or written here.
    const response=await context.request.get(base+'/api/ai/research-trackers',{timeout:30000});
    assert.equal(response.status(),200,'research-trackers API');
    const data=await response.json();
    assert.equal(data.trackers.length,16);
    assert(data.diagnostics && data.diagnostics.reversal && data.diagnostics.coinglass);
    const filtered=data.trackers.find(r=>r.id==='xs_reversal_liq_filter');
    assert.equal(filtered.metric_kind,'paired_increment');
    assert.equal(filtered.return_basis,'total_gross_notional');
    const pairs=data.diagnostics.reversal;
    if(pairs.n_pairs) assert(Math.abs(pairs.shadow.mean_pct-pairs.baseline.mean_pct-pairs.increment.mean_pp)<1e-8);
    const attribution=await context.request.get(base+'/api/ai/coinglass-attribution',{timeout:30000});
    assert.equal(attribution.status(),200,'coinglass-attribution API');
    const cells=await attribution.json();
    assert.equal(Object.keys(cells.cells).length,4);
    await page.waitForFunction(()=>document.querySelectorAll('#research-trackers-tbody tr').length===16,{timeout:30000});
    const html=await page.locator('#research-tracker-diagnostics').innerText();
    assert(html.includes('反转过滤 · 相同日期对照'));
    assert(html.includes('Coinglass · 币池与权重的四格归因'));
    assert(html.includes('131币 · IC加权') && html.includes('234币 · 等权'));
    await page.locator('#research-tracker-diagnostics').scrollIntoViewIfNeeded();
    await page.screenshot({path:path.join(output,'desktop.png')});
    const desktopOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2);
    await page.setViewportSize({width:430,height:900});
    const narrowOverflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2);
    const overflowElements=narrowOverflow?await page.evaluate(()=>Array.from(document.querySelectorAll('body *'))
      .map(e=>({tag:e.tagName,id:e.id,cls:typeof e.className==='string'?e.className:'',right:Math.round(e.getBoundingClientRect().right),width:Math.round(e.getBoundingClientRect().width),overflow:getComputedStyle(e).overflowX}))
      .filter(e=>e.right>innerWidth+2&&e.width>innerWidth).slice(0,12)):[];
    await page.screenshot({path:path.join(output,'narrow.png')});
    assert.equal(pageErrors.length,0,'uncaught JavaScript errors');
    const receipt={ok:true,tracker_count:16,paired_days:pairs.n_pairs,coinglass_cells:4,
      common_coinglass_dates:cells.factorial_common.n_pairs,
      registered_risk_shadows:data.diagnostics.event_risk.filter(s=>s.registered).length,
      page_errors:pageErrors.length,desktop_document_overflow:desktopOverflow,narrow_document_overflow:narrowOverflow,
      narrow_overflow_elements:overflowElements,
      checked_at:new Date().toISOString()};
    fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2));
    process.stdout.write(JSON.stringify(receipt)+'\n');
  }finally{await context.close();await browser.close();}
}
main().catch(e=>{process.stderr.write(e.name+': '+e.message+'\n');process.exitCode=1;});
