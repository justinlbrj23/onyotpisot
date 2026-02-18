// inspectWebpage.cjs
// Stage 2: evaluation + filtration with stealth hardening
// Requires:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis p-limit

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio'); // fallback only
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { google } = require('googleapis');
const pLimit = require('p-limit');

puppeteer.use(StealthPlugin());

// =========================
// CONFIG (env overrides)
const SERVICE_ACCOUNT_FILE = process.env.SERVICE_ACCOUNT_FILE || './service-account.json';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = process.env.SHEET_NAME || 'web_tda';
const URL_RANGE = process.env.URL_RANGE || 'C2:C';

const OUTPUT_ELEMENTS_FILE = process.env.OUTPUT_ELEMENTS_FILE || 'raw-elements.ndjson';
const OUTPUT_ROWS_FILE = process.env.OUTPUT_ROWS_FILE || 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = process.env.OUTPUT_ERRORS_FILE || 'errors.json';
const OUTPUT_SUMMARY_FILE = process.env.OUTPUT_SUMMARY_FILE || 'summary.json';

const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || '25000');
const MAX_NODES_PER_PAGE = parseInt(process.env.MAX_NODES_PER_PAGE || '20000', 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '3', 10);
const PAGE_TIMEOUT = parseInt(process.env.PAGE_TIMEOUT || '120000', 10);

// =========================
// GOOGLE AUTH
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// HELPERS
function parseCurrency(str) {
  if (!str) return null;
  const s = String(str).replace(/\s+/g, '');
  const million = /([\d,.]+)M$/i.exec(s);
  if (million) return parseFloat(million[1].replace(/,/g, '')) * 1e6;
  const n = parseFloat(s.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function hashString(s) {
  return crypto.createHash('sha1').update(String(s || '')).digest('hex');
}

function appendNdjson(file, obj) {
  fs.appendFileSync(file, JSON.stringify(obj) + '\n');
}

// =========================
// LOAD URLS
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
// BROWSER UTILITIES
async function setupRequestInterception(page) {
  try {
    await page.setRequestInterception(true);
    page.on('request', req => {
      const r = req.resourceType();
      const url = req.url();
      if (
        r === 'image' ||
        r === 'font' ||
        r === 'stylesheet' ||
        /analytics|doubleclick|googlesyndication|google-analytics|ads|tracking/i.test(url)
      ) {
        req.abort();
      } else {
        req.continue();
      }
    });
  } catch (e) {
    // some puppeteer versions may not support interception on certain contexts
  }
}

async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise(resolve => {
      let total = 0;
      const distance = 400;
      const timer = setInterval(() => {
        window.scrollBy(0, distance);
        total += distance;
        if (total > document.body.scrollHeight) {
          clearInterval(timer);
          resolve();
        }
      }, 200);
    });
  });
}

// =========================
// IN-PAGE EXTRACTION
const IN_PAGE_EXTRACTOR = `
(() => {
  function cssPath(el) {
    if (!el) return '';
    const parts = [];
    while (el && el.nodeType === 1 && el.tagName.toLowerCase() !== 'html') {
      let part = el.tagName.toLowerCase();
      if (el.id) part += '#' + el.id;
      else if (el.className && typeof el.className === 'string') {
        const cls = el.className.trim().split(/\\s+/).slice(0,2).join('.');
        if (cls) part += '.' + cls;
      }
      let nth = 1;
      let p = el.previousElementSibling;
      while (p) { if (p.tagName === el.tagName) nth++; p = p.previousElementSibling; }
      if (nth > 1) part += ':nth-of-type(' + nth + ')';
      parts.unshift(part);
      el = el.parentElement;
    }
    return parts.join(' > ');
  }

  function isVisible(el) {
    try {
      const style = window.getComputedStyle(el);
      if (!style) return false;
      if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) === 0) return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    } catch (e) { return false; }
  }

  function collectFromRoot(root, cap) {
    const out = [];
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, null, false);
    let node;
    let index = 0;
    while ((node = walker.nextNode()) && index < cap) {
      index++;
      try {
        const tag = node.tagName.toLowerCase();
        const text = (node.innerText || '').replace(/\\s+/g,' ').trim().slice(0,2000);
        const attrs = {};
        for (let i=0;i<node.attributes.length;i++){
          const a = node.attributes[i];
          attrs[a.name] = a.value;
        }
        const rect = node.getBoundingClientRect();
        out.push({
          tag,
          cssPath: cssPath(node),
          text,
          innerHTML: node.innerHTML ? node.innerHTML.slice(0,2000) : '',
          attrs,
          dataset: node.dataset || {},
          role: node.getAttribute('role') || '',
          aria: Object.keys(node.attributes || {}).filter(n=>n.startsWith('aria-')).reduce((acc,k)=>{acc[k]=node.getAttribute(k);return acc;},{ }),
          visible: isVisible(node),
          rect: { x: rect.x, y: rect.y, w: rect.width, h: rect.height }
        });
      } catch(e) { /* ignore node errors */ }
    }
    return out;
  }

  const cap = ${MAX_NODES_PER_PAGE};
  let results = collectFromRoot(document, cap);

  const all = Array.from(document.querySelectorAll('*'));
  for (const el of all) {
    if (el.shadowRoot) {
      try {
        results = results.concat(collectFromRoot(el.shadowRoot, cap - results.length));
        if (results.length >= cap) break;
      } catch(e) {}
    }
  }

  const iframes = Array.from(document.querySelectorAll('iframe'));
  for (const f of iframes) {
    try {
      const doc = f.contentDocument;
      if (doc) {
        const frameResults = collectFromRoot(doc, cap - results.length);
        for (const r of frameResults) r.frameUrl = f.src || location.href;
        results = results.concat(frameResults);
        if (results.length >= cap) break;
      } else {
        results.push({
          tag: 'iframe',
          cssPath: cssPath(f),
          text: '',
          innerHTML: '',
          attrs: { src: f.src || '' },
          dataset: {},
          role: f.getAttribute('role') || '',
          aria: {},
          visible: isVisible(f),
          rect: (function(){ const rect = f.getBoundingClientRect(); return { x: rect.x, y: rect.y, w: rect.width, h: rect.height }; })(),
          frameUrl: f.src || ''
        });
      }
    } catch(e) {
      results.push({
        tag: 'iframe',
        cssPath: cssPath(f),
        text: '',
        innerHTML: '',
        attrs: { src: f.src || '' },
        dataset: {},
        role: f.getAttribute('role') || '',
        aria: {},
        visible: isVisible(f),
        rect: { x:0,y:0,w:0,h:0 },
        frameUrl: f.src || ''
      });
    }
  }

  return results.slice(0, cap);
})();
`;

// =========================
// PARSING UTILITIES (lightweight fallback)
// =========================
function normalizeText(s) {
  if (!s) return '';
  return String(s).replace(/\s+/g, ' ').replace(/\u00A0/g, ' ').trim();
}

// =========================
// PAGE PARSING + HIGHER-LEVEL ROW EXTRACTION
async function inspectAndParse(page, url) {
  const pageResult = {
    relevantElements: [],
    parsedRows: [],
    error: null
  };

  try {
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });
    await autoScroll(page);
    await page.waitForTimeout(1200);

    const nodes = await page.evaluate(IN_PAGE_EXTRACTOR);

    const auctionTextRegex = /(\$[0-9]{1,3}(?:,[0-9]{3})+)|\bAPN\b|\bParcel\b|\bAuction\b|\bCase\b/i;
    const relevant = [];
    const seen = new Set();

    for (const n of nodes) {
      const text = (n.text || '').replace(/\s+/g, ' ').trim();
      const attrsString = JSON.stringify(n.attrs || {});
      const key = hashString(n.cssPath + '|' + text.slice(0,200));
      if (seen.has(key)) continue;
      seen.add(key);

      if (auctionTextRegex.test(text) || /apn|parcel|auction|case/i.test(attrsString)) {
        const out = {
          sourceUrl: url,
          tag: n.tag,
          cssPath: n.cssPath,
          text,
          innerHTML: n.innerHTML,
          attrs: n.attrs,
          dataset: n.dataset,
          role: n.role,
          aria: n.aria,
          visible: n.visible,
          rect: n.rect,
          frameUrl: n.frameUrl || ''
        };
        relevant.push(out);
        appendNdjson(OUTPUT_ELEMENTS_FILE, out);
      }
    }

    // Build candidate containers by grouping relevant nodes by parent path heuristics
    const containerCandidates = new Map();
    for (const r of relevant) {
      const parts = (r.cssPath || '').split(' > ');
      if (parts.length <= 1) continue;
      const parentPath = parts.slice(0, Math.max(1, parts.length - 1)).join(' > ');
      if (!containerCandidates.has(parentPath)) containerCandidates.set(parentPath, r.text);
      else containerCandidates.set(parentPath, containerCandidates.get(parentPath) + ' | ' + r.text);
    }

    // For each container, attempt sold-specific parsing first, then generic extraction
    for (const [containerPath, sampleText] of containerCandidates.entries()) {
      const blockText = normalizeText(sampleText);
      if (!blockText) continue;

      // SOLD-specific heuristics (coarse)
      const looksSold = /sold|auction sold|sale finalized|finalized/i.test(blockText);
      if (looksSold) {
        // Try to extract common fields
        const openingBidMatch = blockText.match(/Opening Bid\s*:?\\s*\\$[0-9,]+(?:\\.[0-9]{2})?/i);
        const openingBid = openingBidMatch ? openingBidMatch[0].replace(/Opening Bid\s*:?/i, '').trim() : '';

        const assessedValueMatch = blockText.match(/Assessed Value\s*:?\\s*\\$[0-9,]+(?:\\.[0-9]{2})?/i);
        const assessedValue = assessedValueMatch ? assessedValueMatch[0].replace(/Assessed Value\s*:?/i, '').trim() : '';

        // Sale price: try multiple labels
        const SALE_PRICE_LABELS = ['Amount','Sale Price','Sold Amount','Winning Bid','Final Bid','Sale Amount','Sold Price','Sold For'];
        let salePrice = '';
        for (const label of SALE_PRICE_LABELS) {
          const regex = new RegExp(label.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&') + '\\s*:?\\s*\\$[0-9,]+(?:\\.[0-9]{2})?', 'i');
          const m = blockText.match(regex);
          if (m) {
            const raw = m[0];
            const cleaned = raw.replace(new RegExp(label + '\\s*:?\\s*', 'i'), '').trim();
            salePrice = cleaned;
            break;
          }
        }
        if (!salePrice) {
          const moneyMatch = blockText.match(/\\$[0-9,]+(?:\\.[0-9]{2})?/g);
          if (moneyMatch && moneyMatch.length) salePrice = moneyMatch[moneyMatch.length - 1];
        }

        const parcelMatch = blockText.match(/APN\\s*[:#]?\\s*([0-9A-Za-z-]+)/i) || blockText.match(/Parcel ID\\s*[:#]?\\s*([0-9A-Za-z-]+)/i);
        const parcelId = parcelMatch ? parcelMatch[1].trim() : '';

        if (!parcelId || !openingBid) {
          // fallback to generic parsing below
        } else {
          const open = parseCurrency(openingBid);
          const assess = parseCurrency(assessedValue);
          const soldPrice = parseCurrency(salePrice);

          let surplus = null;
          if (assess !== null) {
            if (soldPrice !== null) surplus = assess - soldPrice;
            else if (open !== null) surplus = assess - open;
          }

          pageResult.parsedRows.push({
            sourceUrl: url,
            containerPath,
            auctionStatus: 'Sold',
            auctionType: (blockText.match(/Auction Type\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
            caseNumber: (blockText.match(/Case\\s*#?\\s*[:#]?\\s*([^|\\n]+)/i) || [])[1] || '',
            parcelId,
            propertyAddress: (blockText.match(/Property Address\\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
            openingBid,
            assessedValue,
            auctionDate: (blockText.match(/Date\\/?Time\\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
            salePrice,
            surplus,
            meetsMinimumSurplus: surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No'
          });
          continue;
        }
      }

      // Generic extraction fallback
      const openingBidMatch = blockText.match(/Opening Bid\s*:?\\s*\\$[0-9,]+(?:\\.[0-9]{2})?/i);
      const openingBid = openingBidMatch ? openingBidMatch[0].replace(/Opening Bid\s*:?/i, '').trim() : '';

      const assessedValueMatch = blockText.match(/Assessed Value\s*:?\\s*\\$[0-9,]+(?:\\.[0-9]{2})?/i);
      const assessedValue = assessedValueMatch ? assessedValueMatch[0].replace(/Assessed Value\s*:?/i, '').trim() : '';

      const salePriceMatch = blockText.match(/\\$[0-9,]+(?:\\.[0-9]{2})?/g);
      const salePrice = salePriceMatch ? salePriceMatch[salePriceMatch.length - 1] : '';

      const parcelMatch = blockText.match(/APN\\s*[:#]?\\s*([0-9A-Za-z-]+)/i) || blockText.match(/Parcel ID\\s*[:#]?\\s*([0-9A-Za-z-]+)/i);
      const parcelId = parcelMatch ? parcelMatch[1].trim() : '';

      if (!parcelId || !openingBid) continue;

      const open = parseCurrency(openingBid);
      const soldPrice = parseCurrency(salePrice);
      const assess = parseCurrency(assessedValue);

      let surplus = null;
      if (assess !== null) {
        if (soldPrice !== null) surplus = assess - soldPrice;
        else if (open !== null) surplus = assess - open;
      }

      pageResult.parsedRows.push({
        sourceUrl: url,
        containerPath,
        auctionStatus: /sold/i.test(blockText) ? 'Sold' : 'Active',
        auctionType: (blockText.match(/Auction Type\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
        caseNumber: (blockText.match(/Case\\s*#?\\s*[:#]?\\s*([^|\\n]+)/i) || [])[1] || '',
        parcelId,
        propertyAddress: (blockText.match(/Property Address\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
        openingBid,
        assessedValue,
        auctionDate: (blockText.match(/Date\\/?Time\s*:?\\s*([^|\\n]+)/i) || [])[1] || '',
        salePrice,
        surplus,
        meetsMinimumSurplus: surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No'
      });
    }

    pageResult.relevantElements = relevant;
    console.log(`📦 Elements found: ${relevant.length} | Auctions parsed: ${pageResult.parsedRows.length}`);
    return pageResult;
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err && err.message ? err.message : err);
    pageResult.error = { url, message: err && err.message ? err.message : String(err) };
    return pageResult;
  }
}

// =========================
// SCRAPE PAGINATED URLS
async function scrapeAllPages(browser, startUrl) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });
  try { await page.emulateTimezone('America/New_York'); } catch (e) {}
  await setupRequestInterception(page);

  const allRows = [];
  const errors = [];

  let pageIndex = 1;
  while (true) {
    const currentUrl = pageIndex === 1 ? startUrl : `${startUrl}&page=${pageIndex}`;
    const result = await inspectAndParse(page, currentUrl);
    if (result.error) errors.push(result.error);
    allRows.push(...result.parsedRows);

    if (!result.relevantElements.length && !result.parsedRows.length) {
      console.log('⛔ No more pages or no relevant content found');
      break;
    }

    pageIndex++;
    if (pageIndex > 50) {
      console.log('⚠️ Reached page limit, stopping.');
      break;
    }
    console.log(`➡️ Moving to page ${pageIndex}`);
    await page.waitForTimeout(500 + Math.floor(Math.random() * 800));
  }

  await page.close();
  return { allRows, errors };
}

// =========================
// MAIN
(async () => {
  console.log('📥 Loading URLs...');
  const urls = await loadTargetUrls();
  if (!urls.length) {
    console.log('No URLs found. Exiting.');
    process.exit(0);
  }

  try { fs.unlinkSync(OUTPUT_ELEMENTS_FILE); } catch(e) {}
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, '');

  const limit = pLimit(CONCURRENCY);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
    ],
  });

  const allRows = [];
  const allErrors = [];

  const tasks = urls.map(url => limit(async () => {
    try {
      const res = await scrapeAllPages(browser, url);
      allRows.push(...res.allRows);
      allErrors.push(...res.errors);
    } catch (e) {
      allErrors.push({ url, message: e && e.message ? e.message : String(e) });
    }
  }));

  await Promise.all(tasks);

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(allRows, null, 2));
  if (allErrors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(allErrors, null, 2));
    console.log(`⚠️ Saved ${allErrors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  const summary = {
    totalUrls: urls.length,
    totalElements: (() => {
      try {
        const lines = fs.readFileSync(OUTPUT_ELEMENTS_FILE, 'utf8').trim().split('\n').filter(Boolean);
        return lines.length;
      } catch (e) { return 0; }
    })(),
    totalRowsFinal: allRows.length,
    errorsCount: allErrors.length,
    surplusAboveThreshold: allRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: allRows.filter(r => r.meetsMinimumSurplus === 'No').length,
  };

  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  console.log(`✅ Saved elements (ndjson) → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${allRows.length} auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();