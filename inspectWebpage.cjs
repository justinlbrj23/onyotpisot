// inspectWebpage.cjs
// Requires:
// npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';

const MIN_SURPLUS = 25000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Load URLs
// =========================
async function loadTargetUrls() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  return (res.data.values || [])
    .flat()
    .map(v => v.trim())
    .filter(v => v.startsWith('http'));
}

// =========================
// Helpers
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function withPage(url, pageNum) {
  const u = new URL(url);
  u.searchParams.set('PAGE', pageNum);
  return u.toString();
}

function extractTotalPages(html) {
  const m = html.match(/page\s+\d+\s+of\s+(\d+)/i);
  return m ? parseInt(m[1], 10) : 1;
}

// =========================
// Inspect + Parse ONE PAGE
// =========================
async function inspectSinglePage(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 8000));

    const html = await page.content();

    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);

    // ----------------------------
    // Capture relevant elements
    // ----------------------------
    const relevantElements = [];
    const auctionTextRegex =
      /(\$\d{1,3}(,\d{3})+)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)/i;

    $('*').each((_, el) => {
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      if (text && auctionTextRegex.test(text)) {
        relevantElements.push({
          sourceUrl: url,
          tag: el.tagName,
          text,
          attrs: el.attribs || {},
        });
      }
    });

    // ----------------------------
    // SOLD + SURPLUS PARSER
    // ----------------------------
    const parsedRows = [];

    $('div').each((_, container) => {
      const blockText = $(container).text().replace(/\s+/g, ' ').trim();
      if (!blockText.includes('Auction Sold')) return;

      const extract = label => {
        const r = new RegExp(`${label}\\s*:?\\s*([^\\n$]+)`, 'i');
        const m = blockText.match(r);
        return m ? m[1].trim() : '';
      };

      const saleMatch = blockText.match(/Amount:\s*\$[\d,]+\.\d{2}/i);
      const assessMatch = blockText.match(/Assessed Value:\s*\$[\d,]+\.\d{2}/i);
      if (!saleMatch || !assessMatch) return;

      const sale = parseCurrency(saleMatch[0]);
      const assess = parseCurrency(assessMatch[0]);
      if (sale === null || assess === null) return;

      const surplus = assess - sale;
      if (surplus < MIN_SURPLUS) return;

      const parcelId = $(container).find('a').first().text().trim();
      const caseNumber = extract('Case #');
      if (!parcelId || !caseNumber) return;

      parsedRows.push({
        sourceUrl: url,
        auctionStatus: 'Sold',
        auctionType: extract('Auction Type') || 'Tax Sale',
        caseNumber,
        parcelId,
        propertyAddress: extract('Property Address'),
        salePrice: sale,
        assessedValue: assess,
        surplus,
        meetsMinimumSurplus: 'Yes',
      });
    });

    const totalPages = extractTotalPages(html);
    console.log(`📦 SOLD+SURPLUS: ${parsedRows.length} | Pages: ${totalPages}`);

    return { relevantElements, parsedRows, totalPages };
  } catch (err) {
    return { relevantElements: [], parsedRows: [], totalPages: 1, error: err.message };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN (WITH PAGINATION)
// =========================
(async () => {
  console.log('📥 Loading URLs...');
  const urls = await loadTargetUrls();

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
    ],
  });

  const allElements = [];
  const allRows = [];
  const errors = [];
  const dedupe = new Set();

  for (const baseUrl of urls) {
    console.log('➡️ Page 1');
    const first = await inspectSinglePage(browser, withPage(baseUrl, 1));
    allElements.push(...first.relevantElements);

    for (const r of first.parsedRows) {
      const k = `${r.caseNumber}|${r.parcelId}`;
      if (!dedupe.has(k)) {
        dedupe.add(k);
        allRows.push(r);
      }
    }

    for (let p = 2; p <= first.totalPages; p++) {
      console.log(`➡️ Page ${p}`);
      const res = await inspectSinglePage(browser, withPage(baseUrl, p));
      allElements.push(...res.relevantElements);

      for (const r of res.parsedRows) {
        const k = `${r.caseNumber}|${r.parcelId}`;
        if (!dedupe.has(k)) {
          dedupe.add(k);
          allRows.push(r);
        }
      }
      if (res.error) errors.push({ url: baseUrl, page: p, error: res.error });
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(allRows, null, 2));
  if (errors.length) fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log(`✅ Saved ${allRows.length} SOLD + SURPLUS auctions`);
  console.log('🏁 Done');
})();