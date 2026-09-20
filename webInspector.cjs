// webInspector.cjs
// Page intelligence + RealForeclose auction parser
// Requires: npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1DvpL59xxVVihpRVhxKUomCpugHLPeokvjKVWKMPAcnw';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';
const OUTPUT_SUMMARY_FILE = 'summary.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// URL LOADING
// =========================
function normalizeAmpersands(value) {
  return String(value || '')
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&');
}

async function loadTargetUrls() {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  return (response.data.values || [])
    .flat()
    .map(value => String(value || '').trim())
    .filter(value => /^https?:\/\//i.test(value))
    .map(normalizeAmpersands);
}

// =========================
// GENERAL HELPERS
// =========================
function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function parseCurrency(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;

  const text = String(value).trim();
  if (!text) return null;

  const parsed = Number.parseFloat(text.replace(/[^0-9.-]/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeLabel(value) {
  return clean(value)
    .toLowerCase()
    .replace(/\s*#\s*$/, '')
    .replace(/\s*:\s*$/, '')
    .trim();
}

function getByLabel($, $context, labels) {
  const targets = (Array.isArray(labels) ? labels : [labels]).map(normalizeLabel);
  let result = '';

  $context.find('th, td').each((_, element) => {
    if (result) return false;

    const $element = $(element);
    const elementText = normalizeLabel($element.clone().children().remove().end().text() || $element.text());
    const matched = targets.some(target => elementText === target);
    if (!matched) return;

    const $next = $element.next('td, th');
    if ($next.length) {
      result = clean($next.text());
      if (result) return false;
    }

    const cells = $element.closest('tr').find('th, td').toArray();
    const index = cells.indexOf(element);
    if (index >= 0 && cells[index + 1]) {
      result = clean($(cells[index + 1]).text());
      if (result) return false;
    }
  });

  return result;
}

function extractTextField(blockText, labels, stopLabels = []) {
  const labelList = Array.isArray(labels) ? labels : [labels];

  for (const label of labelList) {
    const start = escapeRegex(label);
    const stops = stopLabels.map(escapeRegex).join('|');
    const pattern = stops
      ? new RegExp(`${start}\\s*#?\\s*:\\s*(.*?)(?=(?:${stops})\\s*#?\\s*:|$)`, 'i')
      : new RegExp(`${start}\\s*#?\\s*:\\s*(.+)$`, 'i');

    const match = blockText.match(pattern);
    if (match && clean(match[1])) return clean(match[1]);
  }

  return '';
}

function isRealParcelId(value) {
  const normalized = clean(value).toLowerCase();
  if (!normalized) return false;

  return !new Set([
    'property appraiser', 'parcel id', 'parcel number', 'account number',
    'n/a', 'na', 'none', 'unknown', '-'
  ]).has(normalized);
}

function extractAuctionDateFromUrl(url) {
  try {
    const parsed = new URL(normalizeAmpersands(url));
    for (const [key, value] of parsed.searchParams.entries()) {
      if (key.toLowerCase() === 'auctiondate') return clean(value);
    }
  } catch {}
  return '';
}

function splitAddressAndCityZip(value) {
  let propertyAddress = clean(value);
  let cityStateZip = '';

  const match = propertyAddress.match(/([A-Za-z .'-]+),?\s+([A-Za-z]{2})\s*-?\s*(\d{5}(?:-\d{4})?)\s*$/i);
  if (match) {
    cityStateZip = `${clean(match[1])}, ${match[2].toUpperCase()} ${match[3]}`;
    propertyAddress = clean(propertyAddress.slice(0, match.index));
  }

  return { propertyAddress, cityStateZip };
}

// =========================
// PAGE/PAGER HELPERS
// =========================
async function getPageScopes(page) {
  await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 60000 });
  await page.waitForSelector('#BID_WINDOW_CONTAINER div[aid]', { timeout: 60000 });

  return {
    async firstRowSignature() {
      return page.$eval('#BID_WINDOW_CONTAINER', root => {
        const first = root.querySelector('div[aid]');
        return first ? `${first.getAttribute('aid') || ''}|${first.innerText || first.textContent || ''}` : '__NONE__';
      });
    },

    async waitForListChange(previousSignature, timeoutMs = 30000) {
      const started = Date.now();
      while (Date.now() - started < timeoutMs) {
        try {
          const current = await this.firstRowSignature();
          if (current !== previousSignature) return true;
        } catch {}
        await sleep(400);
      }
      return false;
    },

    async getPagerPieces() {
      const bar = await page.$('#BID_WINDOW_CONTAINER .Head_C > div:nth-of-type(3)');
      if (!bar) return { bar: null, input: null, text: null, next: null };

      const input = await bar.$("input[type='text'], input[type='number'], input:not([type])");
      const text = await bar.$('span.PageText');
      const next =
        await bar.$('span.PageRight > img') ||
        await bar.$('.PageRight_HVR > img') ||
        await bar.$('.PageRight img') ||
        await bar.$('img[alt*="next" i], img[title*="next" i]');

      return { bar, input, text, next };
    },

    async readIndicator(pieces) {
      if (!pieces?.bar) return { current: null, total: null, raw: '' };

      return page.evaluate(bar => {
        const input = bar.querySelector('input');
        const text = bar.querySelector('span.PageText');
        const inputValue = (input?.value || '').trim();
        const raw = (text?.innerText || text?.textContent || '').replace(/\s+/g, ' ').trim();
        const totalMatch = raw.match(/\bof\s*([0-9]+)/i);

        return {
          current: /^\d+$/.test(inputValue) ? Number(inputValue) : null,
          total: totalMatch ? Number(totalMatch[1]) : null,
          raw,
        };
      }, pieces.bar);
    },

    async setPageInputAndGo(pieces, nextPage) {
      if (!pieces?.input) return false;

      try {
        await pieces.input.focus();
        await page.evaluate((element, value) => {
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
          if (setter) setter.call(element, String(value));
          else element.value = String(value);
          element.dispatchEvent(new Event('input', { bubbles: true }));
          element.dispatchEvent(new Event('change', { bubbles: true }));
        }, pieces.input, nextPage);
        await page.keyboard.press('Enter');
        return true;
      } catch {
        return false;
      }
    },

    async clickNextArrow(pieces) {
      if (!pieces?.next) return false;
      try {
        await page.evaluate(element => {
          element.scrollIntoView({ block: 'center' });
          const clickable = element.closest('a, button, input[type="submit"], input[type="button"]');
          (clickable || element).click();
        }, pieces.next);
        return true;
      } catch {
        try {
          await pieces.next.click({ delay: 20 });
          return true;
        } catch {
          return false;
        }
      }
    },
  };
}

// =========================
// AUCTION PARSER
// =========================
function parseAuctionsFromHtml(html, pageUrl) {
  const $ = cheerio.load(html);
  const rows = [];
  const relevant = [];

  $('#BID_WINDOW_CONTAINER div[aid]').each((_, item) => {
    const $item = $(item);
    const blockText = clean($item.text());
    const auctionId = clean($item.attr('aid'));
    const itemClass = clean($item.attr('class'));

    relevant.push({
      sourceUrl: pageUrl,
      tag: 'div',
      attrs: $item.attr() || {},
      text: blockText.slice(0, 5000),
    });

    let caseNumber = getByLabel($, $item, ['Cause Number', 'Case Number', 'Case #']);
    let openingBid = getByLabel($, $item, [
      'Est. Min. Bid', 'Estimated Minimum Bid', 'Opening Bid', 'Final Judgment Amount'
    ]);
    let parcelId = getByLabel($, $item, [
      'Account Number', 'Parcel Number', 'Parcel ID', 'Parcel #'
    ]);
    let propertyAddress = getByLabel($, $item, ['Property Address']);
    let assessedValue = getByLabel($, $item, ['Adjudged Value', 'Assessed Value']);
    let cityStateZip = getByLabel($, $item, ['City, State Zip', 'City/State/Zip', 'City State Zip']);

    caseNumber ||= extractTextField(blockText,
      ['Case #', 'Case Number', 'Cause Number'],
      ['Final Judgment Amount', 'Parcel ID', 'Parcel Number', 'Property Address', 'Assessed Value', 'Plaintiff Max Bid']);

    openingBid ||= extractTextField(blockText,
      ['Est. Min. Bid', 'Estimated Minimum Bid', 'Opening Bid', 'Final Judgment Amount'],
      ['Parcel ID', 'Parcel Number', 'Account Number', 'Property Address', 'Assessed Value', 'Plaintiff Max Bid']);

    parcelId ||= extractTextField(blockText,
      ['Parcel ID', 'Parcel Number', 'Account Number'],
      ['Property Address', 'Assessed Value', 'Plaintiff Max Bid', 'Final Judgment Amount']);

    propertyAddress ||= extractTextField(blockText,
      ['Property Address'],
      ['Assessed Value', 'Plaintiff Max Bid', 'Parcel ID', 'Final Judgment Amount']);

    assessedValue ||= extractTextField(blockText,
      ['Adjudged Value', 'Assessed Value'],
      ['Plaintiff Max Bid', 'Opening Bid', 'Est. Min. Bid']);

    if (!cityStateZip && propertyAddress) {
      const split = splitAddressAndCityZip(propertyAddress);
      propertyAddress = split.propertyAddress;
      cityStateZip = split.cityStateZip;
    }

    const status =
      clean($item.find('div.ASTAT_MSGA').first().text()) ||
      clean($item.find('.status, .ASTAT_MSGA').first().text());

    const soldAmount =
      clean($item.find('div.ASTAT_MSGD').first().text()) ||
      clean($item.find('.ASTAT_MSGD').first().text());

    const statusLower = status.toLowerCase();
    const classLower = itemClass.toLowerCase();
    const looksSold =
      statusLower.includes('sold') ||
      statusLower.includes('paid') ||
      classLower.includes('sold') ||
      (Boolean(soldAmount) && parseCurrency(soldAmount) !== null);

    const row = {
      sourceUrl: pageUrl,
      auctionId,
      auctionStatus: looksSold ? 'Sold' : status || (classLower.includes('preview') ? 'Preview' : 'Unknown'),
      auctionType: 'Foreclosure',
      caseNumber: clean(caseNumber),
      parcelId: isRealParcelId(parcelId) ? clean(parcelId) : '',
      propertyAddress: clean(propertyAddress),
      openingBid: clean(openingBid),
      salePrice: clean(soldAmount),
      assessedValue: clean(assessedValue),
      auctionDate: extractAuctionDateFromUrl(pageUrl),
      cityStateZip: clean(cityStateZip),
      status: clean(status),
      rawClass: itemClass,
    };

    if (!row.caseNumber) {
      relevant.push({
        sourceUrl: pageUrl,
        tag: 'parse-rejection',
        attrs: { aid: auctionId, reason: 'MissingCaseNumber' },
        text: blockText.slice(0, 5000),
      });
      return;
    }

    const openingBidNum = parseCurrency(row.openingBid);
    const assessedNum = parseCurrency(row.assessedValue);
    const salePriceNum = parseCurrency(row.salePrice);

    row.surplus = salePriceNum !== null && openingBidNum !== null
      ? salePriceNum - openingBidNum
      : null;
    row.surplusAssessVsSale = assessedNum !== null && salePriceNum !== null
      ? assessedNum - salePriceNum
      : null;
    row.surplusSaleVsOpen = salePriceNum !== null && openingBidNum !== null
      ? salePriceNum - openingBidNum
      : null;
    row.meetsMinimumSurplus = row.surplus !== null && row.surplus >= MIN_SURPLUS ? 'Yes' : '';

    rows.push(row);
  });

  return { rows, relevant };
}

// =========================
// INSPECT ONE URL
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const relevantElements = [];
  const parsedRows = [];
  const seen = new Set();

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    const normalizedUrl = normalizeAmpersands(url);
    await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: 120000 });
    const scope = await getPageScopes(page);

    let pagesVisited = 0;
    while (pagesVisited < MAX_PAGES) {
      const html = await page.content();
      const { rows, relevant } = parseAuctionsFromHtml(html, normalizedUrl);

      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}|${row.auctionId}`;
        if (!seen.has(key)) {
          seen.add(key);
          parsedRows.push(row);
        }
      }
      relevantElements.push(...relevant);
      console.log(`   Parsed rows so far: ${parsedRows.length}`);

      const pieces = await scope.getPagerPieces();
      if (!pieces.bar) {
        console.log('Pager bar not found. Ending this URL.');
        break;
      }

      const indicator = await scope.readIndicator(pieces);
      const current = indicator.current || pagesVisited + 1;
      const total = indicator.total;
      if (total && current >= total) {
        console.log(`Reached last page (${current}/${total}).`);
        break;
      }

      const previousSignature = await scope.firstRowSignature();
      const nextPage = current + 1;
      let changed = false;

      if (await scope.setPageInputAndGo(pieces, nextPage)) {
        changed = await scope.waitForListChange(previousSignature);
      }

      if (!changed && pieces.next && await scope.clickNextArrow(pieces)) {
        changed = await scope.waitForListChange(previousSignature);
      }

      if (!changed) {
        console.log('No list change after pager actions. Ending this URL.');
        break;
      }

      pagesVisited += 1;
    }

    return { relevantElements, parsedRows };
  } catch (error) {
    console.error(`Error on ${url}:`, error.message || error);
    return {
      relevantElements,
      parsedRows,
      error: { url, message: error.message || String(error) },
    };
  } finally {
    try { await page.close(); } catch {}
  }
}

// =========================
// MAIN
// =========================
(async () => {
  let browser;

  try {
    console.log('Loading URLs...');
    const urls = await loadTargetUrls();
    console.log(`Got ${urls.length} URL(s) to process.`);

    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled', '--ignore-certificate-errors',
        '--disable-gpu', '--no-zygote'
      ],
    });

    const allElements = [];
    const allRows = [];
    const errors = [];

    for (const url of urls) {
      console.log(`Processing: ${url}`);
      const result = await inspectAndParse(browser, url);
      allElements.push(...result.relevantElements);
      allRows.push(...result.parsedRows);
      if (result.error) errors.push(result.error);
    }

    const unique = new Map();
    for (const row of allRows) {
      const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}|${row.auctionId}`;
      if (!unique.has(key)) unique.set(key, row);
    }
    const finalRows = [...unique.values()];

    const summary = {
      generatedAt: new Date().toISOString(),
      totalUrls: urls.length,
      totalElements: allElements.length,
      totalRowsRaw: allRows.length,
      totalRowsFinal: finalRows.length,
      errorsCount: errors.length,
      soldRows: finalRows.filter(row => row.auctionStatus === 'Sold').length,
      previewRows: finalRows.filter(row => row.auctionStatus === 'Preview').length,
      surplusAboveThreshold: finalRows.filter(row => row.surplus !== null && row.surplus >= MIN_SURPLUS).length,
      surplusBelowThreshold: finalRows.filter(row => row.surplus !== null && row.surplus < MIN_SURPLUS).length,
      blanks: {
        parcelIdBlank: finalRows.filter(row => !row.parcelId).length,
        salePriceBlank: finalRows.filter(row => !row.salePrice).length,
        auctionDateBlank: finalRows.filter(row => !row.auctionDate).length,
        surplusBlank: finalRows.filter(row => row.surplus === null).length,
      },
    };

    fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
    fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
    fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

    if (errors.length) fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    else if (fs.existsSync(OUTPUT_ERRORS_FILE)) fs.unlinkSync(OUTPUT_ERRORS_FILE);

    console.log(`Saved ${allElements.length} elements -> ${OUTPUT_ELEMENTS_FILE}`);
    console.log(`Saved ${finalRows.length} parsed auctions -> ${OUTPUT_ROWS_FILE}`);
    console.log(`Saved summary -> ${OUTPUT_SUMMARY_FILE}`);
    console.log('Done.');
  } catch (error) {
    console.error('Fatal webInspector error:', error.message || error);
    process.exitCode = 1;
  } finally {
    if (browser) {
      try { await browser.close(); } catch {}
    }
  }
})();
