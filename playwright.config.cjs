/** @type {import('@playwright/test').PlaywrightTestConfig} */
module.exports = {
  testDir: './tests',
  testMatch: /playwright_smoke\.spec\.js$/,
  timeout: 180000,
  retries: 0,
  reporter: 'line',
  use: {
    headless: true,
    trace: 'off',
    screenshot: 'only-on-failure',
    video: 'off',
  },
};
