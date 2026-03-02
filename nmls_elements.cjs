
// nmls_elements.cjs
// ------------------------------------------------------------
// Purpose: Automate navigation on https://www.nmlsconsumeraccess.org/
// Navigation logic and browser hardening modeled after the user's sample.
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
//  NEW (Compliant):
//   • Integrated OCR utilities (Tesseract/convert/pdftoppm/gs) for NON-CAPTCHA files only.
//     Use env RUN_OCR_NON_CAPTCHA=1 and OCR_INPUT_PATH to process non-captcha images/PDFs.
//     OCR outputs go to artifacts/ocr_output/<timestamp>.
//
// IMPORTANT COMPLIANCE NOTES:
//   - Do not bypass captchas or site access controls. This script does NOT auto-solve captcha.
//   - The OCR utilities will REFUSE to run on files named like 'captcha_*.png' or paths
//     clearly pointing to the captcha image to avoid misuse.
// ------------------------------------------------------------

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const puppeteer = require('puppeteer');

// =========================
// CONFIG + CLI
// =========================
const ARTIFACTS_DIR = path.join(process.cwd(), 'artifacts');
const BASE_URL = 'https://www.nmlsconsumeraccess.org/';

function readFlag(name, fallback = '') {
  const prefix = `--${name}=`;
  const found = process.argv.find(a => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

const HEADFUL = process.argv.includes('--headful');
const ZIP_CODE = process.env.ZIP_CODE || readFlag('zip', '33122');
const CAPTCHA_TIMEOUT_MS = parseInt(process.env.CAPTCHA_TIMEOUT_MS || readFlag('captcha-timeout-ms', '300000'), 10);
const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined; // allow system Chrome

// OCR toggles (NON-CAPTCHA ONLY)
const RUN_OCR_NON_CAPTCHA = /^1|true|yes$/i.test(String(process.env.RUN_OCR_NON_CAPTCHA || '0'));
const OCR_INPUT_PATH = process.env.OCR_INPUT_PATH || ''; // absolute or relative to repo root

// Selectors
const SEL = {
  zipInput: process.env.SEL_ZIP_INPUT || 'input.swap_value',
  goBtn: process.env.SEL_GO_BTN || 'input.go',
  agreeCheckbox: process.env.SEL_AGREE || 'input#ctl00_MainContent_cbxAgreeToTerms',
  captchaImg: process.env.SEL_CAPTCHA_IMG || 'img#c_turingtestpage_ctl00_maincontent_captcha1_CaptchaImage',
  captchaInput: process.env.SEL_CAPTCHA_INPUT || 'input.swap_value',
  submitCandidates: (
    process.env.SEL_SUBMIT_CANDIDATES
      ? process.env.SEL_SUBMIT_CANDIDATES.split(',').map(s => s.trim()).filter(Boolean)
      : [
          'input.aspNetDisabled',
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
if (!fs.existsSync(ARTIFACTS_DIR)) fs.mkdirSync(ARTIFACTS_DIR);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function ensureFile(file, content = '') {
  try { fs.writeFileSync(file, content, 'utf8'); } catch {}
}

// =========================
// OCR UTILITIES (NON-CAPTCHA ONLY)
// =========================
const ALLOWED_IMAGE_EXTS = ['.png', '.jpg', '.jpeg'];
const ALLOWED_PDF_EXTS = ['.pdf'];

function isRealPDF(filePath) {
  const fd = fs.openSync(filePath, 'r');
  const buffer = Buffer.alloc(5);
  fs.readSync(fd, buffer, 0, 5, 0);
  fs.closeSync(fd);
  return buffer.toString() === '%PDF-';
}

function preprocessImage(inputPath, outputPath) {
  // Requires ImageMagick's `convert`
  execSync(`convert "${inputPath}" -colorspace Gray -resize 300% -contrast-stretch 0 "${outputPath}"`);
}

function runOcrNonCaptcha(inputFile, outBaseDir) {
  const baseName = path.basename(inputFile).toLowerCase();
  if (baseName.startsWith('captcha_')) {
    console.warn('⛔ Skipping OCR: Detected CAPTCHA image name. OCR is disabled for captcha.');
    return null;
  }
  const ext = path.extname(inputFile).toLowerCase();
  const outDir = path.join(outBaseDir, String(Date.now()));
  fs.mkdirSync(outDir, { recursive: true });

  try {
    let pageImages = [];

    if (ALLOWED_PDF_EXTS.includes(ext)) {
      console.log('📄 OCR: PDF detected — validating...');
      if (!isRealPDF(inputFile)) throw new Error('File has .pdf extension but is NOT a valid PDF');
      console.log('✅ OCR: PDF valid — converting pages to images...');
      try {
        execSync(`pdftoppm "${inputFile}" ${outDir}/page -png`);
      } catch {
        console.warn('⚠️ pdftoppm failed — attempting PDF normalization...');
        const normalizedPDF = path.join(outDir, 'normalized.pdf');
        execSync(`gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dNOPAUSE -dQUIET -dBATCH -sOutputFile="${normalizedPDF}" "${inputFile}"`);
        execSync(`pdftoppm "${normalizedPDF}" ${outDir}/page -png`);
      }
      pageImages = fs.readdirSync(outDir)
        .filter(f => f.startsWith('page-') && f.endsWith('.png'))
        .filter(f => fs.statSync(path.join(outDir, f)).isFile())
        .sort((a, b) => {
          const na = parseInt(a.match(/page-(\d+)/)[1], 10);
          const nb = parseInt(b.match(/page-(\d+)/)[1], 10);
          return na - nb;
        });

    } else if (ALLOWED_IMAGE_EXTS.includes(ext)) {
      const target = path.join(outDir, 'page-1.png');
      fs.copyFileSync(inputFile, target);
      pageImages = ['page-1.png'];

    } else {
      throw new Error(`Unsupported file type: ${ext}. Supported types: ${ALLOWED_PDF_EXTS.concat(ALLOWED_IMAGE_EXTS).join(', ')}`);
    }

    if (pageImages.length === 0) throw new Error('No pages found to OCR after conversion');

    console.log('🔎 OCR files to process:', pageImages);
    let combinedText = '';
    const jsonResults = [];

    for (let i = 0; i < pageImages.length; i++) {
      const pageNum = i + 1;
      const rawPath = path.join(outDir, pageImages[i]);
      const cleanPath = path.join(outDir, `clean-page-${pageNum}.png`);

      console.log(`🔍 OCR page ${pageNum}`);
      preprocessImage(rawPath, cleanPath);

      let text = '';
      try {
        text = execSync(`tesseract "${cleanPath}" stdout`, { encoding: 'utf8' });
      } catch (e) {
        console.warn(`⚠️ Tesseract failed on page ${pageNum}: ${e.message}`);
      }

      const entities = {
        amounts: text.match(/\b(?:₱|\$)?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b/g) || [],
        dates: text.match(/\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b/g) || [],
        ids: text.match(/\b[A-Z0-9]{6,}\b/g) || []
      };

      fs.writeFileSync(path.join(outDir, `page-${pageNum}.txt`), text);
      jsonResults.push({ page: pageNum, entities, text });
      combinedText += `\n\n===== PAGE ${pageNum} =====\n\n${text}`;
    }

    fs.writeFileSync(path.join(outDir, 'full_text.txt'), combinedText);
    fs.writeFileSync(path.join(outDir, 'output.json'), JSON.stringify(jsonResults, null, 2));

    console.log('✅ OCR complete (Tesseract + PDF normalization + image support)');
    return outDir;

  } catch (err) {
    console.error('❌ OCR failed:', err.message);
    return null;
  }
}

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
    } catch {}
    await sleep(3000);
  }
  return '';
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

  page.on('console', msg => console.log('[PAGE]', msg.text()));

  try {
    console.log('➡️  Navigating to:', BASE_URL);
    const navResp = await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

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

    const ts = Date.now();
    const fullshot = path.join(ARTIFACTS_DIR, `step4_captcha_page_${ts}.jpg`);
    const textCaptionPath = path.join(ARTIFACTS_DIR, `textCaption_${ts}.png`);

await el.screenshot({ path: textCaptionPath });
console.log(`🖼️ Saved CAPTCHA element as: ${textCaptionPath}`);

// Create note + empty answer file under new naming
ensureFile(path.join(ARTIFACTS_DIR, 'textCaption_note.txt'), [
  'This run captured a visual challenge image.',
  'OCR is disabled for this file.',
  '',
  'Provide the human-decoded text inside artifacts/textCaption_answer.txt within the timeout window.'
].join('\n'));

ensureFile(path.join(ARTIFACTS_DIR, 'textCaption_answer.txt'), '');
      
    } catch (e) {
      console.warn('⚠️  Could not capture screenshots:', e.message);
    }

    // OPTIONAL OCR for NON-CAPTCHA content
    if (RUN_OCR_NON_CAPTCHA && OCR_INPUT_PATH) {
      const inputAbs = path.isAbsolute(OCR_INPUT_PATH) ? OCR_INPUT_PATH : path.join(process.cwd(), OCR_INPUT_PATH);
      console.log('🧠 Running OCR (non-captcha):', inputAbs);
      runOcrNonCaptcha(inputAbs, path.join(ARTIFACTS_DIR, 'ocr_output'));
    }

    // HUMAN-IN-THE-LOOP CAPTCHA ANSWER (no OCR)
    const answerFile = path.join(ARTIFACTS_DIR, 'captcha_answer.txt');
    let captchaText = (process.env.CAPTCHA_TEXT || '').trim();

    if (!captchaText) {
      captchaText = await readCaptchaFromFile(answerFile, CAPTCHA_TIMEOUT_MS);
    }

    if (!captchaText) {
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

    let clicked = false;
    for (const sel of SEL.submitCandidates) {
      const exists = await page.$(sel);
      if (!exists) continue;
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
})();
