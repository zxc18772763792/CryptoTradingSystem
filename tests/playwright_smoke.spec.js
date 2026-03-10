const { test, expect } = require('@playwright/test');

const BASE_URL = 'http://127.0.0.1:8000';

function hasMojibake(text) {
  const raw = String(text || '');
  if (!raw) return false;
  if (raw.includes('\uFFFD')) return true;
  return /(å|ç|æ|ä|é|è|ö|Ã|Â){3,}/.test(raw);
}

async function collectVisiblePageState(page, scope = 'body') {
  return await page.locator(scope).evaluate((node) => {
    const text = (node && node.innerText) ? node.innerText : '';
    const all = Array.from((node || document.body).querySelectorAll('*'));
    const visibleTextNodes = all
      .filter((el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      })
      .map((el) => (el.innerText || '').trim())
      .filter(Boolean);
    const loadingTexts = visibleTextNodes.filter((line) => line.includes('加载中'));
    const errorNotifications = Array.from(document.querySelectorAll('.notification.error,.error,.toast-error'))
      .map((el) => (el.innerText || '').trim())
      .filter(Boolean);
    return {
      text,
      loadingTexts,
      errorNotifications,
      headings: Array.from(document.querySelectorAll('h1,h2,h3'))
        .map((el) => (el.innerText || '').trim())
        .filter(Boolean),
    };
  });
}

test.describe('crypto trading ui smoke', () => {
  test.setTimeout(180000);

  test('browse main pages without obvious display issues', async ({ page }) => {
    const requestFailures = [];
    const pageErrors = [];
    const consoleErrors = [];

    page.on('requestfailed', (req) => {
      requestFailures.push(`${req.method()} ${req.url()} :: ${req.failure()?.errorText || 'failed'}`);
    });
    page.on('pageerror', (err) => pageErrors.push(String(err)));
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });

    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
    await expect(page.locator('h1')).toContainText('加密交易系统');
    await page.waitForTimeout(5000);

    const tabTargets = [
      { button: '仪表盘', content: '#dashboard' },
      { button: '交易', content: '#trading' },
      { button: '策略', content: '#strategies' },
      { button: '数据', content: '#data' },
      { button: '高级研究', content: '#research' },
      { button: 'AI研究', content: '#ai-research' },
      { button: '回测', content: '#backtest' },
    ];

    const findings = [];

    for (const tab of tabTargets) {
      await page.getByRole('button', { name: tab.button, exact: true }).click();
      await page.waitForTimeout(3500);
      await expect(page.locator(tab.content)).toBeVisible();
      const state = await collectVisiblePageState(page, tab.content);
      findings.push({
        area: tab.button,
        headings: state.headings.slice(0, 6),
        loadingCount: state.loadingTexts.length,
        mojibake: hasMojibake(state.text),
        errors: state.errorNotifications,
      });
    }

    await page.goto(`${BASE_URL}/news`, { waitUntil: 'domcontentloaded' });
    await expect(page.locator('h1')).toContainText('新闻中心');
    await page.waitForTimeout(6000);
    const newsState = await collectVisiblePageState(page);
    findings.push({
      area: '/news',
      headings: newsState.headings.slice(0, 8),
      loadingCount: newsState.loadingTexts.length,
      mojibake: hasMojibake(newsState.text),
      errors: newsState.errorNotifications,
    });

    console.log(JSON.stringify({
      findings,
      requestFailures,
      pageErrors,
      consoleErrors,
    }, null, 2));

    const mojibakeAreas = findings.filter((item) => item.mojibake).map((item) => item.area);
    const displayErrors = findings.filter((item) => item.errors.length > 0);
    expect(mojibakeAreas, `visible mojibake in: ${mojibakeAreas.join(', ')}`).toEqual([]);
    expect(displayErrors, 'visible error notifications should be empty').toEqual([]);
    expect(pageErrors, 'pageerror should be empty').toEqual([]);
    expect(requestFailures, 'requestfailed should be empty').toEqual([]);
    expect(consoleErrors, 'console error should be empty').toEqual([]);
  });
});
