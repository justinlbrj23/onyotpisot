// =========================
// Requires:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis
// =========================

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const cheerio = require('cheerio');
const { google } = require('googleapis');
const fs = require('fs').promises;
const path = require('path');

// =========================
// CONFIG
// =========================

const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CAEdjXisPmgAHmv3qo3y1LBYktQftLKHk-LK04_oKes';
const SHEET_RANGE = 'Sheet1!A:D';
const TARGET_URL = 'https://www.nmlsconsumeraccess.org/';

// =========================
// GOOGLE AUTH
// =========================

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Helpers
// =========================

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function fibonacciDelays(n, base = 7) {
  const seq = [base, base];
  for (let i = 2; i < n; i++) {
    seq[i] = seq[i - 1] + seq[i - 2];
  }
  return seq;
}

async function humanLikeMoveAndClick(page, selector) {
  const el = await page.$(selector);
  if (!el) throw new Error("Target element not found");

  const box = await el.boundingBox();
  if (!box) throw new Error("Bounding box not available");

  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;

  for (let i = 0; i < 10; i++) {
    const jitterX = x + (Math.random() - 0.5) * 5;
    const jitterY = y + (Math.random() - 0.5) * 5;
    await page.mouse.move(jitterX, jitterY, { steps: 3 });
    await sleep(30 + Math.random() * 40);
  }

  await page.mouse.click(x, y, { delay: 100 + Math.random() * 50 });
}

async function humanLikeType(page, selector, text) {
  const delays = fibonacciDelays(text.length, 7);
  for (let i = 0; i < text.length; i++) {
    await page.type(selector, text[i], { delay: delays[i] || 34 });
    await sleep(delays[i] || 34);
  }
}

async function writeFileSafe(name, content, encoding = 'utf8') {
  try {
    await fs.writeFile(name, content, encoding);
    console.log('📝 Wrote debug file:', name);
  } catch (err) {
    console.warn('⚠️ Failed to write debug file:', name, err);
  }
}

async function captureDebugSnapshot(page, tag = 'debug', opts = {}) {
  const ts = Date.now();
  const htmlName = `${tag}-snapshot-${ts}.html`;
  const jsonName = `${tag}-meta-${ts}.json`;
  const jpgName = `${tag}-screenshot-${ts}.jpg`;

  try {
    if (opts.viewport && typeof page.setViewport === 'function') {
      await page.setViewport(opts.viewport).catch(() => null);
    }

    if (typeof page.screenshot === 'function') {
      const screenshotOptions = {
        path: jpgName,
        type: 'jpeg',
        quality: typeof opts.quality === 'number' ? Math.max(10, Math.min(100, opts.quality)) : 80,
        fullPage: !!opts.fullPage,
      };
      await page.screenshot(screenshotOptions).catch(err => {
        console.warn('⚠️ Screenshot failed:', err && err.message);
        try { fs.unlinkSync(jpgName); } catch (e) { /* ignore */ }
      });
    }

    let html = null;
    try { html = await page.content(); } catch (e) { html = null; }

    let inputs = null;
    try {
      inputs = await page.evaluate(() => {
        const arr = Array.from(document.querySelectorAll('input')).map(i => ({
          id: i.id || null,
          name: i.name || null,
          type: i.type || null,
          placeholder: i.placeholder || null,
          class: i.className || null,
          visible: i.offsetParent !== null,
          value: i.value || null
        }));
        return { url: location.href, title: document.title, inputs: arr, timestamp: Date.now() };
      });
    } catch (e) {
      inputs = { error: 'evaluate failed', message: String(e) };
    }

    if (html) await writeFileSafe(htmlName, html);
    await writeFileSafe(jsonName, JSON.stringify(inputs, null, 2));

    let jpgExists = false;
    try {
      const stat = await fs.stat(jpgName).catch(() => null);
      jpgExists = !!stat && stat.size > 0;
    } catch (e) { jpgExists = false; }

    if (jpgExists) {
      console.log('📸 Wrote screenshot:', jpgName);
      return { htmlName: html ? htmlName : null, jsonName, jpgName };
    } else {
      console.warn('⚠️ No screenshot produced for', jpgName);
      return { htmlName: html ? htmlName : null, jsonName, jpgName: null };
    }
  } catch (err) {
    console.warn('⚠️ captureDebugSnapshot failed:', err);
    return { htmlName: null, jsonName: null, jpgName: null };
  }
}

async function detectCaptchaGate(page) {
  const captchaBox = await page.$('input[type="text"]');
  const termsCheckbox = await page.$('input[type="checkbox"]');
  const continueBtn = await page.$('button, input[type="submit"]');

  if (captchaBox && termsCheckbox && continueBtn) {
    console.log("⚠️ CAPTCHA/Terms gate detected.");
    await captureDebugSnapshot(page, 'captcha-gate', { fullPage: true });

    console.log("⏸ Pausing for manual CAPTCHA solve...");
    await page.waitForFunction(() => {
      const btn = document.querySelector('button, input[type="submit"]');
      return btn && btn.disabled === false;
    }, { timeout: 300000 }); // wait up to 5 minutes

    console.log("✅ CAPTCHA solved, continuing...");
    return true;
  }
  return false;
}

// =========================
// Browser Connection Helper
// =========================

async function getConnection() {
  const browser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--enable-javascript',
      '--disable-dev-shm-usage',
'--disable-blink-features=AutomationControlled'
    ],
    defaultViewport: null
  });
  const page = await browser.newPage();

  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

  return { browser, page };
}

// =========================
// Inspect Page
// =========================

async function inspectPage(url) {
  let browser;
  try {
    const { browser: b, page } = await getConnection();
    browser = b;

    console.log("🌐 Navigating...");
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    await page.waitForSelector('body', { timeout: 15000 });

    const html = await page.content();
    const $ = cheerio.load(html);
    const elements = [];

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};
      if (text) {
        elements.push({ tag, text, attrs });
      }
    });

    return elements;
  } catch (err) {
    console.error('❌ Error during page inspection:', err);
    return [];
  } finally {
    if (browser) await browser.close();
  }
}

// =========================
// FUNCTION: Perform Search (robust, race-free, human-like)
// =========================

async function searchPage(url, zipcode) {
  let browser;
  try {
    // Use stealth-enabled connection
    const { browser: b, page } = await getConnection();
    browser = b;

    console.log("🌐 Navigating...");
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    await page.waitForSelector('body', { timeout: 15000 });
    await sleep(5);

    // --- NEW: Detect CAPTCHA/Terms Gate ---
    const captchaBox = await page.$('input[type="text"]');
    const termsCheckbox = await page.$('input[type="checkbox"]');
    const continueBtn = await page.$('button, input[type="submit"]');

    if (captchaBox && termsCheckbox && continueBtn) {
      console.log("⚠️ CAPTCHA/Terms gate detected.");
      await captureDebugSnapshot(page, 'captcha-gate', { fullPage: true });

      console.log("⏸ Pausing for manual CAPTCHA solve...");
      await page.waitForFunction(() => {
        const btn = document.querySelector('button, input[type="submit"]');
        return btn && !btn.disabled;
      }, { timeout: 300000 }); // wait up to 5 minutes

      console.log("✅ CAPTCHA solved, continuing...");
    }

    console.log("📥 Collecting input candidates...");
    const inputs = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('input')).map(i => ({
        id: i.id || null,
        name: i.name || null,
        type: i.type || null,
        placeholder: i.placeholder || null,
        class: i.className || null,
      }));
    });

    let targetSelector = null;
    for (const attrs of inputs) {
      const combined = `${attrs.id || ''} ${attrs.name || ''} ${attrs.placeholder || ''} ${attrs.class || ''}`.toLowerCase();
      if ((attrs.type === 'text' || attrs.type === 'search' || !attrs.type) &&
          (combined.includes('search') || combined.includes('zip') || combined.includes('postal') || combined.includes('city'))) {
        if (attrs.id) targetSelector = `#${attrs.id}`;
        else if (attrs.name) targetSelector = `[name="${attrs.name}"]`;
        else if (attrs.placeholder) targetSelector = `input[placeholder="${attrs.placeholder}"]`;
        else if (attrs.class) targetSelector = `input.${attrs.class.trim().split(/\s+/).join('.')}`;
        break;
      }
    }

    if (!targetSelector) {
      const fallback = await page.evaluate(() => {
        const el = Array.from(document.querySelectorAll('input')).find(i =>
          (i.type === 'text' || i.type === 'search' || !i.type) && i.offsetParent !== null
        );
        if (!el) return null;
        if (el.id) return `#${el.id}`;
        if (el.name) return `[name="${el.name}"]`;
        if (el.placeholder) return `input[placeholder="${el.placeholder}"]`;
        if (el.className) return `input.${el.className.trim().split(/\s+/).join('.')}`;
        return null;
      });
      targetSelector = fallback;
    }

    if (!targetSelector) throw new Error("Could not auto-detect search input field");

    console.log(`🎯 Using input selector: ${targetSelector}`);
    await page.waitForSelector(targetSelector, { timeout: 15000 });

    await page.evaluate(sel => {
      const el = document.querySelector(sel);
      if (el) { el.value = ''; el.focus(); }
    }, targetSelector);

    await humanLikeMoveAndClick(page, targetSelector);
    await sleep(8);

    console.log("⌨️ Typing ZIP with human-like delays...");
    await humanLikeType(page, targetSelector, zipcode);
    await sleep(13);

    const likelyResultsSelector = await page.evaluate(() => {
      const candidates = Array.from(document.querySelectorAll('div, section, ul, table'));
      for (const c of candidates) {
        const id = (c.id || '').toLowerCase();
        const cls = (c.className || '').toLowerCase();
        if (id.includes('result') || cls.includes('result') || id.includes('search') || cls.includes('search') || id.includes('list') || cls.includes('list')) {
          if (c.id) return `#${c.id}`;
          if (c.className) return '.' + c.className.trim().split(/\s+/).join('.');
        }
      }
      return null;
    });

    const navPromise = page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 }).catch(() => null);
    await sleep(21);
    await page.keyboard.press('Enter', { delay: 120 });

    if (likelyResultsSelector) {
      await page.waitForSelector(likelyResultsSelector, { timeout: 15000 }).catch(() => null);
    } else {
      await Promise.race([navPromise, new Promise(res => setTimeout(res, 1200))]);
    }

    await sleep(34);

    const results = await page.evaluate(() => {
      const out = [];
      const containers = Array.from(document.querySelectorAll('div, section, ul, table')).filter(c => {
        const id = (c.id || '').toLowerCase();
        const cls = (c.className || '').toLowerCase();
        return id.includes('result') || cls.includes('result') || id.includes('search') || cls.includes('search') || id.includes('list') || cls.includes('list');
      });

      const parseRow = r => {
        const nameEl = r.querySelector('.resultName, .name, .title, h1, h2, h3');
        const name = (nameEl?.innerText || r.innerText || '').trim();
        if (!name) return null;
        return { name: name.split('\n')[0].trim(), details: r.innerText.trim() };
      };

      if (containers.length > 0) {
        for (const c of containers) {
          const rows = Array.from(c.querySelectorAll('.resultRow, li, tr, .row, .item'));
          if (rows.length === 0) {
            const text = c.innerText.trim();
            if (text) out.push({ name: text.split('\n')[0].trim(), details: text });
          } else {
            for (const r of rows) {
              const parsed = parseRow(r);
              if (parsed) out.push(parsed);
            }
          }
          if (out.length > 0) break;
        }
      } else {
        const items = Array.from(document.querySelectorAll('li, .row, .item, tr')).slice(0, 200);
        for (const it of items) {
          const parsed = parseRow(it);
          if (parsed) out.push(parsed);
        }
      }
      return out;
    });

    console.log(`📦 Found ${results.length} results for ZIP ${zipcode}`);
    console.log('🧪 Sample:', results.slice(0, 5));
    return results;
  } catch (err) {
    console.error('❌ Error during search:', err);
    try {
      if (browser) {
        const pages = await browser.pages();
        if (pages.length > 0) {
          await captureDebugSnapshot(pages[0], 'search-error');
        }
      }
    } catch (snapErr) {
      console.warn('⚠️ Failed to capture debug snapshot:', snapErr);
    }
    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// FUNCTION: Append to Google Sheets
// =========================

async function appendToSheet(results) {
  if (!results || results.length === 0) {
    console.warn('⚠️ No data to write.');
    return;
  }

  const timestamp = new Date().toISOString();
  const first = results[0];
  let values;

  if (first && first.attrs && first.tag && first.text) {
    values = results.map(r => {
      const attrString = Object.entries(r.attrs).map(([k, v]) => `${k}=${v}`).join('; ');
      return [timestamp, r.tag, r.text, attrString];
    });
  } else if (first && (first.name || first.details)) {
    values = results.map(r => [timestamp, r.name || '', r.details || '']);
  } else {
    values = results.map(r => [timestamp, JSON.stringify(r)]);
  }

  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    if (!existing.data.values || existing.data.values.length === 0) {
      const header = (first && first.attrs && first.tag && first.text)
        ? ['Timestamp', 'Tag', 'Text', 'Attributes']
        : (first && (first.name || first.details))
          ? ['Timestamp', 'Name', 'Details']
          : ['Timestamp', 'Data'];
      values.unshift(header);
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values },
    });

    console.log(`✅ Appended ${values.length} rows.`);
  } catch (err) {
    console.error('❌ Sheets error:', err);
  }
}

// =========================
// MAIN
// =========================

(async () => {
  try {
    console.log('🔍 Inspecting webpage...');
    const inspected = await inspectPage(TARGET_URL);
    console.log(`📦 Parsed: ${inspected.length}`);
    console.log('🧪 Sample:', inspected.slice(0, 5));
    console.log('📤 Writing inspected data to Sheets...');
    await appendToSheet(inspected);

    console.log('🔍 Performing search...');
    const searched = await searchPage(TARGET_URL, '33122'); // sample ZIP
    console.log(`📦 Found ${searched.length} search results`);
    console.log('🧪 Sample:', searched.slice(0, 5));
    console.log('📤 Writing search results to Sheets...');
    await appendToSheet(searched);

    console.log('🏁 Done.');
  } catch (err) {
    console.error('❌ Fatal error in main:', err);
  }
})();