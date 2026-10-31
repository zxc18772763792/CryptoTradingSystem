const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require('C:/Users/zxc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
(async()=>{
  const candidates = [chromium.executablePath(),'C:/Program Files/Google/Chrome/Application/chrome.exe','C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'];
  const executablePath = candidates.find(p=>fs.existsSync(p));
  if (!executablePath) throw new Error('No local headless browser executable found');
  const browser = await chromium.launch({headless:true,executablePath});
  const checks=[];
  try {
    for (const [width,colorScheme] of [[736,'light'],[360,'dark']]) {
      const page=await browser.newPage({viewport:{width,height:500},colorScheme});
      await page.goto(pathToFileURL(path.join(__dirname,'chart-preview.html')).href);
      const frame=page.frameLocator('iframe');
      await frame.locator('svg rect[opacity]').first().waitFor({timeout:7000});
      const info=await frame.locator('svg').evaluate(svg=>{
        const box=svg.getBoundingClientRect();
        const labels=[...svg.querySelectorAll('text')];
        return {bars:svg.querySelectorAll('rect[opacity]').length,text:svg.textContent,
          labelsWithin:labels.every(node=>{const b=node.getBoundingClientRect();return b.left>=box.left-1&&b.right<=box.right+1&&b.bottom<=box.bottom+1;})};
      });
      if(info.bars!==2||!info.text.includes('-4.67%')||!info.text.includes('+7.52%')||!info.labelsWithin) throw new Error(JSON.stringify(info));
      await page.screenshot({path:path.join(__dirname,`chart-${width}-${colorScheme}.png`)});
      checks.push({width,colorScheme,...info});
      await page.close();
    }
    fs.writeFileSync(path.join(__dirname,'chart_checks.json'),JSON.stringify(checks,null,2));
    process.stdout.write(JSON.stringify(checks));
  } finally {await browser.close();}
})().catch(error=>{process.stderr.write(error.stack);process.exitCode=1;});
