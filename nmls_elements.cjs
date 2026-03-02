// nmls_elements.cjs
// ------------------------------------------------------------
// Purpose: Automate navigation on https://www.nmlsconsumeraccess.org/
// with navigation logic and browser hardening modeled after the user's sample.
//
// Flow:
//   1) Open homepage and wait for DOMContentLoaded + short delay
//   2) Find input.swap_value, click, and type ZIP
//   3) Find and click input.go
//   4) On next page (after DOMContentLoaded), click the terms checkbox
//      input#ctl00_MainContent_cbxAgreeToTerms
//   5) CAPTCHA: HUMAN-IN-THE-LOOP ONLY (no OCR). The script saves the image to
//      artifacts/captcha_*.png, then tries 3 sources for the answer (in order):
//        a) process.env.CAPTCHA_TEXT
//        b) a drop-file: artifacts/captcha_answer.txt (waits up to X min)
//        c) interactive stdin prompt (for local runs)
//      Then it types the provided text into input.swap_value and attempts submit.
//
// IMPORTANT COMPLIANCE NOTES:
//   - Do not bypass captchas or site access controls. This script does NOT auto-solve captcha.
//   - Selectors can change; inspect and update if needed.
//   - This script avoids force-enabling disabled buttons.
//   - If the site returns 401/403 or a Private Access Token challenge, the script
//     will capture evidence and exit gracefully.
// ------------------------------------------------------------

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// =========================
// CONFIG + CLI
// =========================
const ARTIFACTS_DIR = path.join(process.cwd(), 'artifacts');
const BASE_URL = 'https://www.nmlsconsumeraccess.org/';

// CLI flags: --zip=XXXXX, --headful, --captcha-timeout-ms=NNNN
function readFlag(name, fallback = '') {
  const prefix = `--${name}=`;
  const found = process.argv.find(a => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

const HEADFUL = process.argv.includes('--headful');
const ZIP_CODE = process.env.ZIP_CODE || readFlag('zip', '33122');
const CAPTCHA_TIMEOUT_MS = parseInt(process.env.CAPTCHA_TIMEOUT_MS || readFlag('captcha-timeout-ms', '300000'), 10); // default 5 min
const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined; // allow system Chrome

// Selectors
const SEL = {
  zipInput: process.env.SEL_ZIP_INPUT || 'input.swap_value',
  goBtn: process.env.SEL_GO_BTN || 'input.go',
  agreeCheckbox: process.env.SEL_AGREE || 'input#ctl00_MainContent_cbxAgreeToTerms',
  captchaImg: process.env.SEL_CAPTCHA_IMG || 'img#c_turingtestpage_ctl00_maincontent_captcha1_CaptchaImage',
  captchaInput: process.env.SEL_CAPTCHA_INPUT || 'input.swap_value',
  // A set of possible submit buttons to try after captcha
  submitCandidates: (
    process.env.SEL_SUBMIT_CANDIDATES
      ? process.env.SEL_SUBMIT_CANDIDATES.split(',').map(s => s.trim()).filter(Boolean)
      : [
          'input.aspNetDisabled', // as given (may be disabled unless conditions met)
          'input[type="submit"]',
          'button[type="submit"]',
          'input[value*="Continue" i]',
          'input[value*="Search" i]',
          'input[id*="btn" i]',
          'button[id*="btn" i]'
        ]
  )
};

// Ensure artifacts directory exists
if (!fs.existsSync(ARTIFACTS_DIR)) {
  fs.mkdirSync(ARTIFACTS_DIR);
}

// Small helper delay
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function readCaptchaFromFile(filePath, timeoutMs) {
  const started = Date.now();
  console.log(`⏳ Waiting for captcha answer file: ${filePath} (timeout ${(timeoutMs/1000)|0}s)`);
  while (Date.now() - started < timeoutMs) {
    try {
      if (fs.existsSync(filePath)) {
        const value = fs.readFileSync(filePath, 'utf8').trim();
        if (value) {
          console.log('✅ Captcha answer file detected.');
          return value;
        }
      }
    } catch { /* ignore transient errors */ }
    await sleep(3000);
  }
  return '';
}

async function main() {
  console.log('🚀 Launching browser...');
  const browser = await puppeteer.launch({
    headless: HEADFUL ? false : 'new',
    executablePath,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
      '--disable-gpu',
      '--single-process',
      '--no-zygote'
    ],
    defaultViewport: { width: 1366, height: 900 }
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
    const navResp = await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

    // Guard for 401/403 or blocked states
    try {
      const status = navResp ? navResp.status() : null;
      if (status && (status === 401 || status === 403)) {
        const htmlPath = path.join(ARTIFACTS_DIR, `blocked_${Date.now()}.html`);
        const imgPath = path.join(ARTIFACTS_DIR, `blocked_${Date.now()}.png`);
        try {
          fs.writeFileSync(htmlPath, await page.content(), 'utf-8');
          await page.screenshot({ path: imgPath, fullPage: true });
        } catch {}
        console.error(`❌ Initial navigation blocked with status ${status}. Evidence saved.`);
        await browser.close();
        process.exit(2);
      }
    } catch {}

    await sleep(3000);
    await page.screenshot({ path: path.join(ARTIFACTS_DIR, 'step1_home.jpg'), fullPage: true });

    console.log('🔎 Typing ZIP into input.swap_value...');
    await page.waitForSelector(SEL.zipInput, { visible: true, timeout: 45000 });
    await page.click(SEL.zipInput, { clickCount: 3 });
    await page.type(SEL.zipInput, String(ZIP_CODE), { delay: 40 });

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

    // HUMAN-IN-THE-LOOP CAPTCHA ANSWER (no OCR)
    const answerFile = path.join(ARTIFACTS_DIR, 'captcha_answer.txt');
    let captchaText = (process.env.CAPTCHA_TEXT || '').trim();

    if (!captchaText) {
      // Try drop-file handoff (operator creates artifacts/captcha_answer.txt)
      captchaText = await readCaptchaFromFile(answerFile, CAPTCHA_TIMEOUT_MS);
    }

    if (!captchaText) {
      // Fallback to interactive prompt (best for local runs)
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
      captchaText = await new Promise(resolve => {
        rl.question('Enter CAPTCHA text (check artifacts/captcha_*.png): ', ans => {
          rl.close();
          resolve((ans || '').trim());
        });
      });
    }

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
    try { await page.close(); } catch {}
    await (await browser).close();
    console.log('🏁 Browser closed.');
  }
}

main();
