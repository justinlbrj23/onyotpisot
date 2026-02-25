/**
 * dac_scraper.cjs
 * FULLY REWRITTEN with the robust architecture used in webInspector.cjs
 *
 * Features:
 *  - Retry navigation
 *  - Scoped DOM intelligence
 *  - Anti-bot protections
 *  - Cheerio parsing
 *  - Stable selectors & fallback mechanisms
 *  - Detailed diagnostics
 *  - GitHub Actions optimized
 */

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');

// =========================
// CONFIG
// =========================
const PARCELS_FILE = './parcels.json';          // input list
const OUTPUT_FILE  = './dac_results.json';      // extracted rows
const ERRORS_FILE  = './dac_errors.json';

const MAX_RETRIES = 4;
const NAV_TIMEOUT = 180000;                    // 3 minutes
const WAIT_TIMEOUT = 60000;

// Dallas CAD root
const DAC_URL = "https://www.dallascad.org/AcctDetailRes.aspx?ID=";

// =========================
// Utilities
// =========================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function clean(txt) {
  if (!txt) return '';
  return txt.replace(/\s+/g, ' ').trim();
}

function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

// =========================
// Load Parcel IDs
// =========================
function loadParcelIds() {
  if (!fs.existsSync(PARCELS_FILE)) {
    console.error(`❌ Missing ${PARCELS_FILE}`);
    return [];
  }
  const arr = JSON.parse(fs.readFileSync(PARCELS_FILE, 'utf8'));
  return arr.map(v => String(v).trim()).filter(Boolean);
}

// =========================
// Anti-bot hardening
// =========================
async function preparePage(page) {
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/121.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9'
  });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });
}

// =========================
// Retry navigation
// =========================
async function gotoWithRetries(page, url, retries = MAX_RETRIES) {
  for (let i = 0; i < retries; i++) {
    try {
      return await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: NAV_TIMEOUT
      });
    } catch (err) {
      if (i === retries - 1) throw err;
      console.log(`   🔁 Navigation retry ${i+1}/${retries}...`);
      await sleep(1500);
    }
  }
}

// =========================
// DallasCAD scoped helpers
// =========================
async function getDACScope(page) {
  await page.waitForSelector('#MainContent', { timeout: WAIT_TIMEOUT });

  return {
    async currentHtml() {
      return await page.$eval('#MainContent', el => el.innerHTML);
    },
    async waitForDomChange(prev, timeoutMs = 25000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        try {
          const now = await this.currentHtml();
          if (now !== prev) return true;
        } catch {}
        await sleep(400);
      }
      return false;
    }
  };
}

// =========================
// Parser (Cheerio)
// =========================
function parseDallasCAD(html, url) {
  const $ = cheerio.load(html);
  const result = { sourceUrl: url };

  const findVal = (label) => {
    label = clean(label).toLowerCase();
    let found = '';
    $('table').find('tr').each((_, tr) => {
      const th = clean($(tr).find('th').first().text()).toLowerCase();
      if (th.includes(label)) {
        found = clean($(tr).find('td').first().text());
      }
    });
    return found;
  };

  result.parcelId       = findVal("account");
  result.owner          = findVal("owner");
  result.streetAddress  = findVal("address");
  result.legalDesc      = findVal("legal");
  result.cityStateZip   = findVal("city");
  result.landValue      = findVal("land");
  result.improveValue   = findVal("improvement");
  result.totalValue     = findVal("total");
  result.lastUpdate     = findVal("update");

  return result;
}

// =========================
// Main inspector per parcel
// =========================
async function inspectParcel(browser, parcelId) {
  const url = DAC_URL + parcelId;
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  try {
    await preparePage(page);
    console.log(`🌐 Visiting: ${url}`);

    await gotoWithRetries(page, url);
    const scope = await getDACScope(page);

    const html = await scope.currentHtml();
    const parsed = parseDallasCAD(html, url);

    return { parsed, error: null };

  } catch (err) {
    return { parsed: null, error: { parcelId, url, message: err.message } };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading parcel IDs...');
  const parcels = loadParcelIds();
  console.log(`🧾 Loaded ${parcels.length} parcels`);

  const browser = await puppeteer.launch({
    headless: true,
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

  const finalRows = [];
  const errors = [];

  for (const parcelId of parcels) {
    console.log('==============================');
    console.log(`📌 Processing Parcel: ${parcelId}`);

    const { parsed, error } = await inspectParcel(browser, parcelId);

    if (parsed) {
      finalRows.push(parsed);
      console.log(`   ✔ Extracted tax data`);
    } else {
      errors.push(error);
      console.log(`   ❌ Error: ${error.message}`);
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log('==============================');
  console.log(`✅ Saved results → ${OUTPUT_FILE}`);
  console.log(`⚠️ Saved errors → ${ERRORS_FILE}`);
  console.log('🏁 Done.');
})();