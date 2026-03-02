// nmls_consumer_access.js
// ------------------------------------------------------------
// Purpose: Automate navigation on https://www.nmlsconsumeraccess.org/
// Navigation logic and browser hardening modeled after user's sample.
// Steps:
//   1) Open homepage and wait for DOMContentLoaded + short delay
//   2) Find input.swap_value, click, and type "33122"
//   3) Find and click input.go
//   4) On next page (after DOMContentLoaded), click the terms checkbox
//      input#ctl00_MainContent_cbxAgreeToTerms
//   5) CAPTCHA: human-in-the-loop (no OCR). Save image to artifacts/captcha_*.png,
//      prompt user to type it, enter into input.swap_value, then click a likely submit.
//
// IMPORTANT:
//   - Do not use this to bypass captchas or site controls. Respect Terms of Use.
//   - Selectors can change; inspect and update if needed.
//   - This script avoids programmatically enabling disabled buttons.
// ------------------------------------------------------------

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// =========================
// CONFIG
// =========================
const ARTIFACTS_DIR = path.join(process.cwd(), 'artifacts');
const BASE_URL = 'https://www.nmlsconsumeraccess.org/';
const ZIP_CODE = '33122'; // per user request

// Selectors
const SEL = {
  zipInput: 'input.swap_value',
  goBtn: 'input.go',
  agreeCheckbox: 'input#ctl00_MainContent_cbxAgreeToTerms',
  captchaImg: 'img#c_turingtestpage_ctl00_maincontent_captcha1_CaptchaImage',
  captchaInput: 'input.swap_value', // as specified by user
  // A set of possible submit buttons to try after captcha
  submitCandidates: [
    'input.aspNetDisabled', // as given (may be disabled unless conditions met)
    'input[type="submit"]',
    'button[type="submit"]',
    'input[value*="Continue" i]',
    'input[value*="Search" i]',
    'input[id*="btn" i]',
    'button[id*="btn" i]'
  ]
};

// Ensure artifacts directory exists
if (!fs.existsSync(ARTIFACTS_DIR)) {
  fs.mkdirSync(ARTIFACTS_DIR);
}

// Small helper delay
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =========================
// MAIN
// =========================
(async () => {
  console.log('🚀 Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
      '--disable-gpu',
      '--single-process',
      '--no-zygote'
    ]
  });

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/121.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9'
  });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });

  // Helpful console relay
  page.on('console', msg => console.log('[PAGE]', msg.text()));

  try {
    console.log('➡️  Navigating to:', BASE_URL);
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
    await sleep(3000);
    await page.screenshot({ path: path.join(ARTIFACTS_DIR, 'step1_home.jpg'), fullPage: true });

    console.log('🔎 Typing ZIP into input.swap_value...');
    await page.waitForSelector(SEL.zipInput, { visible: true, timeout: 45000 });
    await page.click(SEL.zipInput, { clickCount: 3 });
    await page.type(SEL.zipInput, ZIP_CODE, { delay: 40 });

    console.log('🖱️ Clicking input.go ...');
    await page.waitForSelector(SEL.goBtn, { visible: true, timeout: 45000 });
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded' }),
      page.click(SEL.goBtn)
    ]);
    await sleep(3000);
    await page.screenshot({ path: path.join(ARTIFACTS_DIR, 'step2_after_go.jpg'), fullPage: true });

    console.log('☑️ Agreeing to terms (if not already checked)...');
    await page.waitForSelector(SEL.agreeCheckbox, { visible: true, timeout: 60000 });
    const isChecked = await page.$eval(SEL.agreeCheckbox, el => el.checked);
    if (!isChecked) {
      await page.click(SEL.agreeCheckbox);
      await sleep(500);
    }
    await page.screenshot({ path: path.join(ARTIFACTS_DIR, 'step3_terms_checked.jpg'), fullPage: true });

    console.log('🧩 Waiting for CAPTCHA image...');
    await page.waitForSelector(SEL.captchaImg, { visible: true, timeout: 60000 });

    // Save captcha and full page for the user
    const ts = Date.now();
    const fullshot = path.join(ARTIFACTS_DIR, `step4_captcha_page_${ts}.jpg`);
    const captchapath = path.join(ARTIFACTS_DIR, `captcha_${ts}.png`);

    try {
      await page.screenshot({ path: fullshot, fullPage: true });
      const el = await page.$(SEL.captchaImg);
      if (el) {
        await el.screenshot({ path: captchapath });
        console.log(`🖼️  Saved CAPTCHA image at: ${captchapath}`);
      }
    } catch (e) {
      console.warn('⚠️  Could not capture screenshots:', e.message);
    }

    // Prompt for manual captcha input
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    const captchaText = await new Promise(resolve => {
      rl.question('Enter CAPTCHA text (check artifacts/captcha_*.png): ', ans => {
        rl.close();
        resolve((ans || '').trim());
      });
    });

    if (!captchaText) {
      console.log('❌ No CAPTCHA entered. Exiting.');
      await browser.close();
      process.exit(0);
    }

    console.log('⌨️  Entering CAPTCHA text into input.swap_value...');
    await page.waitForSelector(SEL.captchaInput, { visible: true, timeout: 60000 });
    await page.click(SEL.captchaInput, { clickCount: 3 });
    await page.type(SEL.captchaInput, captchaText, { delay: 40 });

    // Attempt to click a valid submit/continue button without forcing enable
    let clicked = false;
    for (const sel of SEL.submitCandidates) {
      const exists = await page.$(sel);
      if (!exists) continue;

      // Try clicking only if not disabled/hidden
      const canClick = await page.evaluate((s) => {
        const node = document.querySelector(s);
        if (!node) return false;
        const disabled = node.disabled || node.getAttribute('aria-disabled') === 'true';
        const style = window.getComputedStyle(node);
        const visible = style && style.visibility !== 'hidden' && style.display !== 'none' && style.opacity !== '0';
        return !disabled && visible;
      }, sel);

      if (canClick) {
        console.log(`🖱️ Attempting submit via: ${sel}`);
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 120000 }).catch(() => null),
          page.click(sel)
        ]);
        clicked = true;
        break;
      }
    }

    if (!clicked) {
      console.warn('⚠️  Could not find an enabled submit button. Verify selectors or page state.');
    }

    await sleep(3000);
    await page.screenshot({ path: path.join(ARTIFACTS_DIR, 'step5_after_submit.jpg'), fullPage: true });

    console.log('✅ Flow completed (pending site response).');

  } catch (err) {
    console.error('💥 Automation error:', err);
  } finally {
    await browser.close();
    console.log('🏁 Browser closed.');
  }
})();
