// webInspector.cjs (page intelligence + auction parser for SOLD cards only)
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
const OUTPUT_SUMMARY_FILE = 'summary.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50;
const NAV_TIMEOUT = 120000;
const PAGE_WAIT_MS = 3000;
const DEBUG = true;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// LABEL ALIASES AND NORMALIZATION
// =========================
const LABEL_ALIASES = {
  caseNumber: ['case', 'case number', 'case no', 'cause number', 'cause no', 'cause', 'cause #'],
  parcelId: ['parcel id', 'account number', 'account no', 'account #', 'apn', 'account'],
  openingBid: ['opening bid', 'est min bid', 'est. min. bid', 'est min. bid', 'est. min bid', 'est min'],
  assessedValue: ['assessed value', 'adjudged value', 'adjudged', 'assessed'],
  salePrice: ['sold for', 'sale price', 'final bid', 'winning bid', 'paid amount', 'sold price', 'sold amount'],
  auctionStatus: ['auction status', 'status', 'sale status'],
  propertyAddress: ['property address', 'address', 'property'],
  auctionDate: ['auction date', 'date sold', 'sale date', 'date']
};

function normalizeLabel(raw) {
  return String(raw || '').toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
}

function findCanonicalKey(labelRaw) {
  const nl = normalizeLabel(labelRaw);
  for (const [canon, variants] of Object.entries(LABEL_ALIASES)) {
    if (variants.includes(nl) || nl === canon) return canon;
  }
  return nl;
}

// =========================
// UTILITIES
// =========================
function parseCurrency(str) {
  if (!str && str !== 0) return null;
  const n = parseFloat(String(str).replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function extractDateFlexible(text) {
  if (!text) return '';
  const patterns = [
    /([0-9]{2}\/[0-9]{2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)?)/i,
    /([0-9]{4}-[0-9]{2}-[0-9]{2}(?:\s+[0-9]{2}:[0-9]{2}(?::[0-9]{2})?)?)/i,
  ];
  for (const re of patterns) {
    const m = text.match(re);
    if (m && m[1]) return m[1].trim();
  }
  return '';
}

function extractAmountAfter(text, label) {
  if (!text || !label) return '';
  const safeLabel = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(safeLabel + '\\s*[:#-]?\\s*\\$[\\d,]+(?:\\.\\d{2})?', 'i');
  const m = text.match(regex);
  if (!m) return '';
  const moneyMatch = m[0].match(/\$[\d,]+(?:\.\d{2})?/);
  return moneyMatch ? moneyMatch[0] : '';
}

function extractCurrencyNearLabels(text, labels, window = 80) {
  if (!text) return '';
  const lower = text.toLowerCase();
  for (const label of labels) {
    const idx = lower.indexOf(label.toLowerCase());
    if (idx !== -1) {
      const slice = text.slice(Math.max(0, idx - 20), Math.min(text.length, idx + window));
      const m = slice.match(/\$[\d,]+(?:\.\d{2})?/);
      if (m) return m[0];
    }
  }
  return '';
}

function largestCurrencyInText(text) {
  if (!text) return '';
  const allMoney = [...text.matchAll(/\$[\d,]+(?:\.\d{2})?/g)].map(m => m[0]);
  if (!allMoney.length) return '';
  const sorted = allMoney
    .map(s => ({ s, n: parseCurrency(s) }))
    .filter(x => x.n !== null)
    .sort((a, b) => b.n - a.n);
  return sorted.length ? sorted[0].s : '';
}

// Cross-version sleep helper: pass page instance when available
async function sleep(ms, pageInstance) {
  if (!ms || ms <= 0) return;
  if (pageInstance && typeof pageInstance.waitForTimeout === 'function') {
    await pageInstance.waitForTimeout(ms);
    return;
  }
  await new Promise(r => setTimeout(r, ms));
}

// =========================
// BUILD LABEL→VALUE MAP
// =========================
function buildLabelValueMap($container) {
  const map = {};
  const textNodes = [];

  $container.find('*').each((_, el) => {
    const t = $container.find(el).text().replace(/\s+/g, ' ').trim();
    if (t) textNodes.push(t);
  });

  for (const t of textNodes) {
    const parts = t.split(/[:|-]\s*/);
    if (parts.length >= 2) {
      const labelRaw = parts[0].trim();
      const value = parts.slice(1).join(':').trim();
      const canon = findCanonicalKey(labelRaw);
      if (canon && value) map[canon] = value;
    }
  }

  return { map, text: textNodes.join(' | ') };
}

// =========================
// VALIDATION
// =========================
function validateRow(row) {
  const hasId = (row.caseNumber && row.caseNumber.trim()) || (row.parcelId && row.parcelId.trim());
  const hasSale = row.salePrice && row.salePrice.trim();
  const paidStatus = /paid in full|paid prior to sale|paid/i.test(row.auctionStatus || '');
  return !!(hasId && (hasSale || paidStatus));
}

// =========================
// Helper: extractBetweenFallback for labels not in kv
// =========================
function extractBetweenFallback(text, labels, window = 80) {
  if (!text) return '';
  const lower = text.toLowerCase();
  for (const label of labels) {
    const idx = lower.indexOf(label.toLowerCase());
    if (idx !== -1) {
      const slice = text.slice(idx, Math.min(text.length, idx + window));
      const m = slice.match(/[:#-]?\s*\$?([0-9,]+(?:\.\d{2})?)/);
      if (m) return m[0].replace(/^\s*[:#-]?\s*/, '');
      return slice.replace(new RegExp(label, 'i'), '').trim();
    }
  }
  return '';
}

// =========================
// PAGE PARSER
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  const allRelevantElements = [];
  const allParsedRows = [];
  const seen = new Set();

  try {
    // Anti-bot hardening
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/120.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    let pageIndex = 1;
    while (true) {
      const pageUrl = pageIndex === 1 ? url : `${url}&page=${pageIndex}`;
      console.log(`🌐 Visiting ${pageUrl}`);
      await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
      await sleep(PAGE_WAIT_MS, page);

      const html = await page.content();
      if (
        html.includes('403 Forbidden') ||
        html.includes('Access Denied') ||
        html.toLowerCase().includes('forbidden')
      ) {
        throw new Error('Blocked by target website (403)');
      }

      const $ = cheerio.load(html);

      // Collect diagnostic elements
      const auctionTextRegex =
        /(\$\d{1,3}(,\d{3})+)|(\bAPN\b)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)|(\bWinning Bid\b)|(\bSale Price\b)|(\bAdjudged\b)|(\bEst\.?\s*Min\.?\s*Bid\b)/i;

      $('*').each((_, el) => {
        const tag = el.tagName;
        const text = $(el).text().replace(/\s+/g, ' ').trim();
        const attrs = el.attribs || {};
        if (text && auctionTextRegex.test(text)) {
          allRelevantElements.push({ sourceUrl: pageUrl, tag, text, attrs });
        }
      });

      // Card-based parsing: look for blocks that contain auction-like text
      $('div, li, section, article').each((_, container) => {
        const $container = $(container);
        const blockText = $container.text().replace(/\s+/g, ' ').trim();
        if (!blockText) return;

        // Quick filter: must contain either a case/cause or parcel/account and a status or money
        if (!/case|cause|tx-|parcel|account|apn/i.test(blockText)) return;

        const { map: kv, text: joinedText } = buildLabelValueMap($container);

        // Determine auctionStatus from explicit labels or inline phrases
        const auctionStatus =
          kv['auctionStatus'] ||
          kv['status'] ||
          (blockText.match(/\b(Paid in Full|Paid prior to sale|Paid|Canceled|Pulled for no bids|Sold)\b/i)?.[0] || '');

        // Extract canonical fields using aliases and fallbacks
        const auctionType = kv['auction type'] || kv['sale type'] || '';

        const rawCase =
          kv['caseNumber'] ||
          kv['case'] ||
          extractBetweenFallback(blockText, ['case #', 'cause number', 'cause no', 'cause:'], 40) ||
          '';
        const caseNumber = (rawCase || '').split(/\s+/)[0].trim();

        const parcelRaw =
          kv['parcelId'] ||
          kv['parcel id'] ||
          kv['account number'] ||
          kv['account'] ||
          extractBetweenFallback(blockText, ['parcel id', 'account number', 'account no', 'apn'], 40) ||
          '';
        const parcelId = (parcelRaw || '').split(/\s+/)[0].trim();

        // openingBid from label or Est Min Bid
        const openingBidStr =
          kv['openingBid'] ||
          kv['est min bid'] ||
          extractAmountAfter(blockText, 'Opening Bid') ||
          extractAmountAfter(blockText, 'Est. Min. Bid') ||
          extractCurrencyNearLabels(blockText, ['Opening Bid', 'Est. Min. Bid', 'Est Min Bid'], 120) ||
          '';

        // assessedValue from label or Adjudged Value
        const assessedValueStr =
          kv['assessedValue'] ||
          kv['adjudged value'] ||
          extractAmountAfter(blockText, 'Assessed Value') ||
          extractAmountAfter(blockText, 'Adjudged Value') ||
          extractCurrencyNearLabels(blockText, ['Assessed Value', 'Adjudged Value'], 120) ||
          '';

        // salePrice: multi-strategy
        const salePriceStr =
          kv['salePrice'] ||
          kv['sold for'] ||
          kv['sold price'] ||
          extractAmountAfter(blockText, 'Sold For') ||
          extractAmountAfter(joinedText, 'Sold For') ||
          extractCurrencyNearLabels(blockText, ['Sold For', 'Sale Price', 'Final Bid', 'Winning Bid'], 160) ||
          largestCurrencyInText(blockText) ||
          '';

        const propertyAddress =
          kv['propertyAddress'] || kv['address'] || extractBetweenFallback(blockText, ['property address', 'address'], 80) || '';

        const auctionDate =
          kv['auctionDate'] || extractDateFlexible(blockText) || extractDateFlexible(joinedText) || '';

        const row = {
          sourceUrl: pageUrl,
          auctionStatus: auctionStatus || '',
          auctionType: auctionType || 'Tax Sale',
          caseNumber,
          parcelId,
          propertyAddress,
          openingBid: openingBidStr,
          salePrice: salePriceStr,
          assessedValue: assessedValueStr,
          auctionDate,
          rawText: blockText,
        };

        // Add statusNote if salePrice missing but status present
        if (!row.salePrice && row.auctionStatus) row.statusNote = row.auctionStatus;

        // Validate and log if skipped
        if (!validateRow(row)) {
          if (DEBUG) {
            const missing = [];
            if (!row.caseNumber && !row.parcelId) missing.push('id');
            if (!row.salePrice && !/paid in full|paid prior to sale|paid/i.test(row.auctionStatus || '')) missing.push('salePrice');
            console.log('⛔ Skipping card', {
              sourceUrl: pageUrl,
              missing,
              snippet: blockText.slice(0, 240)
            });
          }
          return;
        }

        // Parse numeric values for surplus calculations
        const open = parseCurrency(row.openingBid);
        const assess = parseCurrency(row.assessedValue);
        const salePrice = parseCurrency(row.salePrice);

        row.surplusAssessVsSale = assess !== null && salePrice !== null ? assess - salePrice : null;
        row.surplusSaleVsOpen = salePrice !== null && open !== null ? salePrice - open : null;
        row.meetsMinimumSurplus = row.surplusAssessVsSale !== null && row.surplusAssessVsSale >= MIN_SURPLUS ? 'Yes' : 'No';

        const dedupeKey = `${pageUrl}|${caseNumber}|${parcelId}`;
        if (seen.has(dedupeKey)) return;
        seen.add(dedupeKey);

        allParsedRows.push(row);
      });

      console.log(`📦 Page ${pageIndex}: Elements ${allRelevantElements.length} | Parsed ${allParsedRows.length}`);

      // Next link detection: prefer rel=next or anchor text 'next'
      let nextLink = $('a[rel="next"]').attr('href');
      if (!nextLink) {
        nextLink = $('a').filter((_, el) => {
          const txt = $(el).text().trim().toLowerCase();
          return txt === 'next' || txt.includes('next');
        }).attr('href');
      }

      if (!nextLink) break;
      pageIndex++;
      if (pageIndex > MAX_PAGES) break;
    }

    return { relevantElements: allRelevantElements, parsedRows: allParsedRows };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { relevantElements: [], parsedRows: [], error: { url, message: err.message } };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs...');
  let urls = [];
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${URL_RANGE}`,
    });
    urls = (res.data.values || []).flat().map(v => v.trim()).filter(v => v.startsWith('http'));
  } catch (err) {
    console.error('❌ Failed to load URLs from spreadsheet:', err.message);
    process.exit(1);
  }

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

  for (const url of urls) {
    try {
      const { relevantElements, parsedRows, error } = await inspectAndParse(browser, url);
      allElements.push(...relevantElements);
      allRows.push(...parsedRows);
      if (error) errors.push(error);
    } catch (err) {
      console.error(`❌ Fatal error on ${url}:`, err.message);
      errors.push({ url, message: err.message });
    }
  }

  await browser.close();

  // Global dedupe
  const uniqueMap = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, row);
  }
  const finalRows = [...uniqueMap.values()];

  // Summary artifact
  const summary = {
    totalUrls: urls.length,
    totalElements: allElements.length,
    totalRowsRaw: allRows.length,
    totalRowsFinal: finalRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'No').length,
    blanks: {
      salePriceBlank: finalRows.filter(r => !r.salePrice).length,
      auctionDateBlank: finalRows.filter(r => !r.auctionDate).length,
    },
  };

  // Write artifacts
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`✅ Saved ${allElements.length} elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${finalRows.length} auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();