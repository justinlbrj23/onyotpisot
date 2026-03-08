// nmls_elements.cjs
// ------------------------------------------------------------
// Purpose:
//   Navigate nmlsconsumeraccess.org until CAPTCHA is shown
//   and save ONLY the CAPTCHA image as textCaption_<ts>.png.
//
// Compliance:
//   - No CAPTCHA bypass
//   - No OCR
//   - Human-in-the-loop only
// ------------------------------------------------------------

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// =========================
// CONFIG
// =========================
const BASE_URL = 'https://www.nmlsconsumeraccess.org/';
const ARTIFACTS_DIR = path.join(process.cwd(), 'artifacts');

const ZIP_CODE =
  process.env.ZIP_CODE ||
  (process.argv.find(a => a.startsWith('--zip=')) || '').split('=')[1] ||
  '33122';

const HEADFUL = process.argv.includes('--headful');
const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined;

// Selectors
const SEL = {
  zipInput: 'input.swap_value',
  goBtn: 'input.go',
  agreeCheckbox: 'input#ctl00_MainContent_cbxAgreeToTerms',
  captchaImg: 'img#c_turingtestpage_ctl00_maincontent_captcha1_CaptchaImage'
};

// Ensure artifacts directory exists
if (!fs.existsSync(ARTIFACTS_DIR)) {
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// =========================
// MAIN
// =========================
(async () => {
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

  try {
    // --------------------------------------------------
    // 1) Open homepage
    // --------------------------------------------------
    console.log('➡️ Navigating to homepage...');
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
    await sleep(3000);

    // --------------------------------------------------
    // 2) Enter ZIP
    // --------------------------------------------------
    console.log('⌨️ Entering ZIP:', ZIP_CODE);
    await page.waitForSelector(SEL.zipInput, { visible: true });
    await page.click(SEL.zipInput, { clickCount: 3 });
    await page.type(SEL.zipInput, ZIP_CODE, { delay: 40 });

    // --------------------------------------------------
    // 3) Click Go
    // --------------------------------------------------
    console.log('🖱️ Clicking Go...');
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded' }),
      page.click(SEL.goBtn)
    ]);

    await sleep(3000);

    // --------------------------------------------------
    // 4) Agree to terms
    // --------------------------------------------------
    console.log('☑️ Agreeing to terms...');
    await page.waitForSelector(SEL.agreeCheckbox, { visible: true });

    const checked = await page.$eval(SEL.agreeCheckbox, el => el.checked);
    if (!checked) {
      await page.click(SEL.agreeCheckbox);
      await sleep(500);
    }

    // --------------------------------------------------
    // 5) Wait for CAPTCHA and save ONLY the image
    // --------------------------------------------------
    console.log('🧩 Waiting for CAPTCHA...');
    await page.waitForSelector(SEL.captchaImg, {
      visible: true,
      timeout: 60000
    });

    const ts = Date.now();
    const outPath = path.join(
      ARTIFACTS_DIR,
      `textCaption_${ts}.png`
    );

    const captchaEl = await page.$(SEL.captchaImg);
    if (!captchaEl) {
      throw new Error('CAPTCHA element not found');
    }

    await captchaEl.screenshot({ path: outPath });
    console.log(`✅ CAPTCHA image saved: ${outPath}`);

    console.log('🏁 Done. No other artifacts generated.');

  } catch (err) {
    console.error('💥 Automation error:', err.message);
  } finally {
    try { await page.close(); } catch {}
    await browser.close();
  }
})();
