// leePA.mjs
// ESM, Selenium, reads addresses from SHEET_NAME!B2:B and target URLs from SHEET_NAME!K2:K
// Classification: iframe present -> detailed account, otherwise results list
// All element waits/use of until.* now use a 30-second timeout constant

import path from 'path';
import { fileURLToPath } from "url";
import fs from 'fs';
import os from "os";
import { randomUUID } from "crypto";
import axios from 'axios';
import https from 'https';
import { google } from 'googleapis';
import { Builder, By, until, Key } from 'selenium-webdriver';
import chrome from 'selenium-webdriver/chrome.js';

// -----------------------------
// Config
// -----------------------------
const SHEET_ID = '19mkuw_zM_054b6zv6uHP98_ijASh-Jl3cYCDnim92I4';
const SHEET_NAME = 'Citrus Springs';
const START_ROW = 13346;
const END_ROW = 14346;
const PAGE_LOAD_TIMEOUT_MS = 30000; // 30s page load
const ELEMENT_TIMEOUT_MS = 10000; // 30s element waits (requested)
const HEADLESS = String(process.env.HEADLESS || 'false').toLowerCase() === 'true';
const CHROME_PATH = process.env.CHROME_PATH || null;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SERVICE_ACCOUNT_PATH = path.join(__dirname, "service-account.json");

// optional throttle: set SHEETS_WRITE_PER_MINUTE env to match your project quota (default 60)
const WRITE_PER_MINUTE = process.env.SHEETS_WRITE_PER_MINUTE ? Number(process.env.SHEETS_WRITE_PER_MINUTE) : 60;

// -----------------------------
// Helpers
// -----------------------------
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function timeoutPromise(ms, message = 'timeout') { return new Promise((_, rej) => setTimeout(() => rej(new Error(message)), ms)); }

async function makeRequestWithRetries(url, retries = 3, backoffFactor = 1000) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const r = await axios.get(url, { httpsAgent: new https.Agent({ rejectUnauthorized: false }), timeout: 60000 });
      console.log(`[HTTP] Reachability check success: ${url} (status ${r.status})`);
      return r;
    } catch (err) {
      console.warn(`[HTTP] Attempt ${attempt + 1} failed for ${url}: ${err.message}`);
      if (attempt + 1 === retries) { console.error(`[HTTP] All retries failed for ${url}`); throw err; }
      await sleep(backoffFactor * 2 ** attempt);
    }
  }
}

// Levenshtein + similarity
function levenshtein(a = '', b = '') {
  const la = a.length, lb = b.length;
  if (la === 0) return lb;
  if (lb === 0) return la;
  const v = Array(lb + 1).fill(0);
  for (let j = 0; j <= lb; j++) v[j] = j;
  for (let i = 1; i <= la; i++) {
    let prev = v[0];
    v[0] = i;
    for (let j = 1; j <= lb; j++) {
      const cur = v[j];
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      v[j] = Math.min(v[j] + 1, v[j - 1] + 1, prev + cost);
      prev = cur;
    }
  }
  return v[lb];
}
// normalize to alpha-numeric only, collapse whitespace, remove leading unit tokens
function normalizeAddressForMatch(s) {
  if (!s) return '';
  // collapse multiple spaces, lowercase
  let t = s.toString().trim().toLowerCase().replace(/\s+/g, ' ');
  // remove common unit prefixes like "unit", "ste", "apt", "#", "suite" and following tokens
  // keep street numbers and core street text
  t = t.replace(/\b(unit|apt|suite|ste|#)\b[:.\s-]*\w*/g, '');
  // remove all non-alphanumeric characters (keep letters and digits only)
  t = t.replace(/[^a-z0-9]/g, '');
  return t;
}

// updated similarity using the same levenshtein implementation you already have
function similarityScore(a, b) {
  a = normalizeAddressForMatch(a);
  b = normalizeAddressForMatch(b);
  if (!a && !b) return 1;
  const dist = levenshtein(a, b);
  const maxLen = Math.max(a.length, b.length);
  return maxLen === 0 ? 1 : (1 - dist / maxLen);
}

// -----------------------------
// Google Sheets
// -----------------------------
async function getSheetsClient() {
  const candidates = [
    SERVICE_ACCOUNT_PATH,
    path.resolve(process.cwd(), 'Property Appraiser Scripts', 'service-account.json'),
    path.resolve(process.cwd(), 'service-account.json'),
  ];
  const keyPath = candidates.find((p) => fs.existsSync(p));
  if (!keyPath) throw new Error(`service-account.json not found. Looked at: ${candidates.join('; ')}`);
  console.log(`[Sheets] Using service account file: ${keyPath}`);
  const auth = new google.auth.GoogleAuth({ keyFile: keyPath, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  return google.sheets({ version: 'v4', auth });
}

// Robust sheets request wrapper with retries + exponential backoff
async function sheetsRequestWithRetries(fn /* async function that performs the sheets op */, opts = {}) {
  const {
    retries = 5,
    initialDelay = 500,
    maxDelay = 30_000,
    retriableStatus = (status) => status === 429 || (status >= 500 && status < 600),
  } = opts;

  let attempt = 0;
  let delay = initialDelay;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      const status = err && err.response && err.response.status;
      const isRetriable = status ? retriableStatus(status) : true; // network errors -> retry
      if (attempt > retries || !isRetriable) {
        throw err;
      }
      console.warn(`[Sheets] Request failed (attempt ${attempt}) status=${status || 'network'} message=${err.message}; backing off ${delay}ms`);
      await sleep(delay);
      delay = Math.min(maxDelay, Math.round(delay * 1.8));
    }
  }
}

// Simple write limiter (token-bucket style by minute window)
let writesThisWindow = 0;
let windowStart = Date.now();
async function throttleWritesIfNeeded() {
  const now = Date.now();
  if (now - windowStart >= 60_000) {
    windowStart = now;
    writesThisWindow = 0;
  }
  if (writesThisWindow >= WRITE_PER_MINUTE) {
    const waitMs = 60_000 - (now - windowStart) + 50;
    console.log(`[Sheets] Throttling writes: sleeping ${waitMs}ms to respect ${WRITE_PER_MINUTE}/min`);
    await sleep(waitMs);
    windowStart = Date.now();
    writesThisWindow = 0;
  }
  writesThisWindow++;
}

// Dismiss a modal if present. Returns true if dismissed, false if not found.
async function dismissPopupModalIfPresent(driver, rowIndex, timeout = 3000) {
  const modalXpath = By.xpath('//*[@id="pnlIssues"]');
  const closeBtnXpath = By.xpath('//*[@id="btnContinue"]');

  try {
    // quick existence check within timeout
    const found = await exists(driver, modalXpath, timeout);
    if (!found) {
      console.log(`[Row ${rowIndex}] No modal found (pnlIssues)`);
      return false;
    }

    // modal present — attempt to click close button
    console.log(`[Row ${rowIndex}] Modal detected (pnlIssues) -> attempting dismiss`);
    try {
      // wait briefly for button to become available and visible
      const btn = await driver.wait(until.elementLocated(closeBtnXpath), Math.min(ELEMENT_TIMEOUT_MS, timeout * 3));
      await driver.wait(until.elementIsVisible(btn), Math.min(ELEMENT_TIMEOUT_MS, timeout * 3));
      await scrollIntoView(driver, btn);
      await btn.click();
      // small pause to allow modal to close
      await sleep(400);
      // confirm it's gone
      if (!(await exists(driver, modalXpath, 1000))) {
        console.log(`[Row ${rowIndex}] Modal dismissed via btnContinue`);
        return true;
      } else {
        console.warn(`[Row ${rowIndex}] Modal still present after click`);
        return false;
      }
    } catch (e) {
      console.warn(`[Row ${rowIndex}] Close button click failed: ${e.message} — attempting JS click fallback`);
      // JS fallback: try to query and click the close button node directly
      try {
        const clicked = await driver.executeScript(
          `const sel = document.evaluate('//*[@id="btnContinue"]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
           if(!sel) return false;
           sel.scrollIntoView({block:'center'});
           sel.click();
           return true;`
        );
        await sleep(400);
        if (clicked && !(await exists(driver, modalXpath, 1000))) {
          console.log(`[Row ${rowIndex}] Modal dismissed via JS fallback`);
          return true;
        } else {
          console.warn(`[Row ${rowIndex}] JS fallback click did not remove modal`);
          return false;
        }
      } catch (e2) {
        console.error(`[Row ${rowIndex}] JS fallback error while dismissing modal: ${e2.message}`);
        return false;
      }
    }
  } catch (e) {
    console.error(`[Row ${rowIndex}] Error checking/dismissing modal: ${e.message}`);
    return false;
  }
}

async function launchDriver() {
  console.log('[Browser] Launching Chrome driver, headless:', HEADLESS);
  const options = new chrome.Options();

  // Generate a unique temporary Chrome user data directory for each run
  const userDataDir = path.join(os.tmpdir(), `chrome-user-data-${randomUUID()}`);
  options.addArguments(`--user-data-dir=${userDataDir}`);

  // Headless or normal mode setup
  if (HEADLESS)
    options.addArguments('--headless=new', '--disable-gpu', '--window-size=1200,900');
  else
    options.addArguments('--start-maximized');

  // General stability flags for CI/CD environments
  options.addArguments(
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-blink-features=AutomationControlled'
  );

  // Detect Chrome binary location
  let chromeBinary = CHROME_PATH;
  if (!chromeBinary) {
    switch (process.platform) {
      case 'win32': {
        const pf = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
        const x86 = 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe';
        chromeBinary = fs.existsSync(pf) ? pf : (fs.existsSync(x86) ? x86 : null);
        break;
      }
      case 'darwin':
        chromeBinary = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
        break;
      default:
        chromeBinary = '/usr/bin/google-chrome-stable';
    }
  }

  if (chromeBinary && fs.existsSync(chromeBinary)) {
    options.setChromeBinaryPath(chromeBinary);
    console.log(`[Browser] Using Chrome binary: ${chromeBinary}`);
  } else {
    console.warn(`[Browser] Chrome binary not found at ${chromeBinary}. Selenium Manager will attempt resolution.`);
  }

  const driver = await new Builder().forBrowser('chrome').setChromeOptions(options).build();
  await driver.manage().setTimeouts({ implicit: 0, pageLoad: PAGE_LOAD_TIMEOUT_MS, script: 60000 });

  if (HEADLESS)
    await driver.manage().window().setRect({ width: 1200, height: 900, x: 0, y: 0 });

  console.log('[Browser] Chrome driver launched');

  // Attach cleanup hook to safely remove temp user-data dir after use
  driver.cleanupUserDataDir = () => {
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true });
      console.log('[Browser] Cleaned up user-data-dir');
    } catch (e) {
      console.warn('[Browser] Failed to clean up user-data-dir:', e.message);
    }
  };

  return driver;
}


// -----------------------------
// DOM helpers (use ELEMENT_TIMEOUT_MS)
// -----------------------------
async function exists(driver, locator, timeout = ELEMENT_TIMEOUT_MS) {
  try { await driver.wait(until.elementLocated(locator), timeout); return true; } catch { return false; }
}
async function getTextSafe(driver, locator, timeout = ELEMENT_TIMEOUT_MS) {
  try { const el = await driver.wait(until.elementLocated(locator), timeout); await driver.wait(until.elementIsVisible(el), timeout); return (await el.getText()).trim(); } catch { return ''; }
}
async function scrollIntoView(driver, element) {
  try { await driver.executeScript('arguments[0].scrollIntoView({block:"center"});', element); } catch {}
}

// -----------------------------
// Page flows (with 30s element waits)
// -----------------------------
async function handleDetailedAccountByIframe(driver, rowIndex) {
  console.log(`[Row ${rowIndex}] handleDetailedAccount: probing iframes for detail link`);
  try {
    const iframes = await driver.findElements(By.css('iframe'));
    console.log(`[Row ${rowIndex}] Found ${iframes.length} iframe(s)`);

    // Attempt to find a frame that contains a main/section or any anchor
    let chosenFrame = null;
    for (let idx = 0; idx < iframes.length; idx++) {
      try {
        await driver.switchTo().frame(iframes[idx]);
        // fast check for a main/section or an anchor inside it
        const hasMain = await exists(driver, By.css('main section, main, body > div > main, #content'), 700);
        const hasAnyAnchor = await exists(driver, By.css('a'), 400);
        await driver.switchTo().defaultContent();
        if (hasMain || hasAnyAnchor) { chosenFrame = idx; break; }
      } catch (e) {
        console.warn(`[Row ${rowIndex}] probe iframe ${idx} error: ${e.message}`);
        try { await driver.switchTo().defaultContent(); } catch {}
      }
    }

    // If none found, but there is at least one iframe, default to index 0
    if (chosenFrame === null && iframes.length > 0) {
      chosenFrame = 0;
      console.log(`[Row ${rowIndex}] No obvious frame matched probes, defaulting to iframe index 0`);
    }

    // If no iframe found at all, still try to operate on the top-level document
    if (chosenFrame !== null) {
      await driver.switchTo().frame(iframes[chosenFrame]);
      console.log(`[Row ${rowIndex}] Switched to iframe (index ${chosenFrame})`);
    } else {
      console.log(`[Row ${rowIndex}] No iframe present, operating on top-level document`);
    }

    // Short micro-settle; prefer tiny waits to long sleeps
    await sleep(300);

    // Candidate selectors (fast CSS first, then forgiving XPaths)
    const candidates = [
      By.xpath('/html/body/div[2]/main/section/div[2]/div[2]/div[3]/div[2]/a'),
      By.css('a[role="button"], a.button, button a, button'),
      By.xpath('//a[contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"), "view")]'),
      By.xpath('//a[contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"), "property")]'),
      By.xpath('//a[contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"), "appraiser")]'),
      By.xpath('//a'), // last resort
    ];

    // Try each candidate selector quickly and click the first workable element
    for (const sel of candidates) {
      try {
        if (!await exists(driver, sel, 1200)) continue;
        const el = await driver.findElement(sel);
        await scrollIntoView(driver, el);
        try {
          await el.click();
          console.log(`[Row ${rowIndex}] Clicked detail anchor via selector ${sel}`);
          await sleep(500);
          return;
        } catch (clickErr) {
          console.warn(`[Row ${rowIndex}] Element.click failed for selector ${sel}: ${clickErr.message} — trying JS click`);
          const clicked = await driver.executeScript(
            `const el = arguments[0]; if(!el) return false; el.scrollIntoView({block:'center'}); el.click(); return true;`,
            el
          );
          if (clicked) {
            console.log(`[Row ${rowIndex}] Clicked detail anchor via JS fallback for selector ${sel}`);
            await sleep(500);
            return;
          }
        }
      } catch (e) {
        console.warn(`[Row ${rowIndex}] Selector ${sel} error: ${e.message}`);
      }
    }

    // If nothing clicked, attempt to find the original specific xpath as a final check
    try {
      const originalXpath = By.xpath('../../div[2]/button');
      if (await exists(driver, originalXpath, 800)) {
        const aEl = await driver.findElement(originalXpath);
        await scrollIntoView(driver, aEl);
        await aEl.click();
        console.log(`[Row ${rowIndex}] Clicked detail anchor via original XPath`);
        await sleep(500);
        return;
      }
    } catch (e) {
      console.warn(`[Row ${rowIndex}] Original XPath attempt failed: ${e.message}`);
    }

    // Nothing worked: capture a small HTML sample for diagnostics then throw
    try {
      const sample = await driver.executeScript(
        `const node = document.querySelector('main') || document.body; return node ? node.outerHTML.slice(0,1200) : '';`
      );
      console.warn(`[Row ${rowIndex}] No detail anchor found; HTML sample: ${sample.slice(0,800)}`);
    } catch (e) {
      console.warn(`[Row ${rowIndex}] Could not capture HTML sample: ${e.message}`);
    }

    throw new Error('Detail anchor not found in detailed account flow');
  } catch (err) {
    // bubble up so caller can handle marking the status
    console.error(`[Row ${rowIndex}] handleDetailedAccountByIframe error: ${err.stack || err.message}`);
    throw err;
  } finally {
    try { await driver.switchTo().defaultContent(); } catch {}
  }
}

async function handleResultsAndMatch(driver, targetAddress, rowIndex) {
  console.log(`[Row ${rowIndex}] handleResults: extracting candidate addresses (30s)`);
  const itemTextCss = 'div.col.pt-2 span.ais-Highlight:nth-of-type(1)';

  // Wait defensively for any candidate nodes
  try {
    await driver.wait(until.elementLocated(By.css(itemTextCss)), ELEMENT_TIMEOUT_MS);
  } catch (e) {
    console.warn(`[Row ${rowIndex}] No candidate nodes located within timeout: ${e.message}`);
    return { matched: false };
  }

  const nodes = await driver.findElements(By.css(itemTextCss));
  const nodeCount = Array.isArray(nodes) ? nodes.length : 0;
  console.log(`[Row ${rowIndex}] Found ${nodeCount} candidate address nodes`);

  if (!Array.isArray(nodes) || nodes.length === 0) return { matched: false };

  // Normalize target once and log for proof
  const normalizedTarget = normalizeAddressForMatch(targetAddress);
  console.log(`[Row ${rowIndex}] Target normalized: "${normalizedTarget}"`);

  for (let i = 0; i < nodes.length; i++) {
    try {
      const rawText = (await nodes[i].getText()).trim();
      const normalizedCandidate = normalizeAddressForMatch(rawText);

      console.log(`[Row ${rowIndex}] Candidate #${i + 1} text: "${rawText}"`);
      console.log(`[Row ${rowIndex}] Candidate #${i + 1} normalized: "${normalizedCandidate}"`);

      // Exact numeric parcel shortcut
      if (/^\d+$/.test(normalizedCandidate) && /^\d+$/.test(normalizedTarget)) {
        if (normalizedCandidate === normalizedTarget) {
          console.log(`[Row ${rowIndex}] Exact numeric parcel match on normalized values`);
          try {
            const ancestorButton = await nodes[i].findElement(By.xpath('../../div[2]/button'));
            await scrollIntoView(driver, ancestorButton);
            await ancestorButton.click();
            console.log(`[Row ${rowIndex}] Clicked matched candidate button`);
            await sleep(600);
            return { matched: true };
          } catch (e) {
            console.warn(`[Row ${rowIndex}] Exact-match click failed: ${e.message} — attempting JS fallback`);
            const btn = await driver.executeScript(
              `const node = arguments[0];
               let el = node;
               for (let j=0;j<8;j++){ if(!el) break; el = el.parentElement; }
               if(!el) return null;
               return el.querySelector('button');`, nodes[i]
            );
            if (btn) {
              await driver.executeScript('arguments[0].scrollIntoView({block:"center"}); arguments[0].click();', btn);
              console.log(`[Row ${rowIndex}] Clicked matched candidate button via JS fallback`);
              await sleep(600);
              return { matched: true };
            } else {
              console.warn(`[Row ${rowIndex}] No clickable button found for exact-match candidate #${i + 1}`);
            }
          }
        } else {
          console.log(`[Row ${rowIndex}] Numeric parcels differ (normalized): "${normalizedCandidate}" vs "${normalizedTarget}"`);
        }
      }

      // Compute similarity on normalized strings
      const score = similarityScore(normalizedCandidate, normalizedTarget);
      console.log(`[Row ${rowIndex}] Similarity (normalized) with target: ${(score * 100).toFixed(1)}%`);

      if (score >= 0.5) {
        console.log(`[Row ${rowIndex}] Candidate #${i + 1} matched (>=50%) — attempting to click associated button (30s lookups)`);
        try {
          const ancestorButton = await nodes[i].findElement(By.xpath('../../div[2]/button'));
          await scrollIntoView(driver, ancestorButton);
          await ancestorButton.click();
          console.log(`[Row ${rowIndex}] Clicked matched candidate button`);
          await sleep(600);
          return { matched: true };
        } catch (e) {
          console.warn(`[Row ${rowIndex}] Failed to click relative button via XPath: ${e.message} — attempting JS fallback`);
          const btn = await driver.executeScript(
            `const node = arguments[0];
             let el = node;
             for (let j=0;j<8;j++){ if(!el) break; el = el.parentElement; }
             if(!el) return null;
             return el.querySelector('button');`, nodes[i]
          );
          if (btn) {
            await driver.executeScript('arguments[0].scrollIntoView({block:"center"}); arguments[0].click();', btn);
            console.log(`[Row ${rowIndex}] Clicked matched candidate button via JS fallback`);
            await sleep(600);
            return { matched: true };
          } else {
            console.warn(`[Row ${rowIndex}] No clickable button found for candidate #${i + 1}`);
          }
        }
      }
    } catch (e) {
      console.warn(`[Row ${rowIndex}] Candidate #${i + 1} processing error: ${e.message}`);
      // continue to next candidate
    }
  }

  console.log(`[Row ${rowIndex}] No matched candidate at >=50% similarity`);
  return { matched: false };
}

// -----------------------------
// extractFromDetail (replaced with batch writes and retries)
// -----------------------------
async function extractFromDetail(driver, sheets, rowIndexZeroBased, ranges) {
  // rowIndexZeroBased is zero-based index in arrays; convert to sheet row
  const row = START_ROW + rowIndexZeroBased;

  // build full A1 addresses by appending the row number
  const dorOwnerA1 = `${ranges.dorOwnerPrefix}${row}`;
  const saleDateA1 = `${ranges.saleDatePrefix}${row}`;
  const soldAmountA1 = `${ranges.soldAmountPrefix}${row}`;
  const mailingAddrA1 = `${ranges.mailingAddrPrefix}${row}`;
  const extraFieldA1 = `${ranges.extraFieldPrefix}${row}`;
  const statusA1 = `${ranges.statusPrefix}${row}`;

  console.log(`[Row ${row}] extractFromDetail: start`);

  // dismiss any blocking modal immediately before scraping
  try {
    await dismissPopupModalIfPresent(driver, row);
  } catch (e) {
    console.warn(`[Row ${row}] dismissPopupModalIfPresent error: ${e.message}`);
  }

  // Local holders for values we'll write
  let dorOwner = '';
  let mailingAddress = '';
  let extraText = '';
  let soldAmount = '';
  let saleDate = '';

  // 1. Owner + Mailing Info
  try {
    const ownerSel = By.css('#datalet_header_row > td > table > tbody > tr.DataletHeaderBottom > td:nth-child(1)');
    const mailingSel = By.css('#datalet_header_row > td > table > tbody > tr.DataletHeaderBottom > td:nth-child(2)');

    console.log(`[Row ${row}] Waiting for owner selector`);
    await driver.wait(until.elementLocated(ownerSel), ELEMENT_TIMEOUT_MS);
    const ownerEl = await driver.findElement(ownerSel);
    await scrollIntoView(driver, ownerEl);
    const ownerRaw = (await ownerEl.getText()).trim();
    const ownerLines = ownerRaw.split('\n').map(s => s.trim()).filter(Boolean);
    dorOwner = ownerLines.join(' + '); // adjust joining logic if needed
    console.log(`[Row ${row}] Owner text:`, ownerLines.slice(0,5).join(' | '));

    console.log(`[Row ${row}] Waiting for mailing selector`);
    await driver.wait(until.elementLocated(mailingSel), ELEMENT_TIMEOUT_MS);
    const mailingEl = await driver.findElement(mailingSel);
    await scrollIntoView(driver, mailingEl);
    const mailingRaw = (await mailingEl.getText()).trim();
    const mailingLines = mailingRaw.split('\n').map(s => s.trim()).filter(Boolean);
    mailingAddress = mailingLines.join(' '); // combine lines into single-line address
    console.log(`[Row ${row}] Mailing address preview:`, mailingLines.slice(0,5).join(' | '));
  } catch (e) {
    console.warn(`[Row ${row}] Owner/mailing extraction failed: ${e.message}`);
  }

  // 2. Extra field (optional)
  try {
    const extraFieldSel = By.css('#datalet_div_6 tr:nth-of-type(2) td:nth-of-type(3)');
    console.log(`[Row ${row}] Looking for extra field`);
    if (await exists(driver, extraFieldSel, ELEMENT_TIMEOUT_MS)) {
      const extraField = await driver.findElement(extraFieldSel);
      await scrollIntoView(driver, extraField);
      extraText = (await extraField.getText()).trim();
      console.log(`[Row ${row}] extraText found: ${String(extraText).slice(0,120)}`);
    } else {
      console.log(`[Row ${row}] No extra field found`);
    }
  } catch (e) {
    console.warn(`[Row ${row}] Extra field extraction failed: ${e.message}`);
  }

  // 3. Sales info
  try {
    const salesLinkSel = By.css('#datalet_div_10');
    console.log(`[Row ${row}] Waiting for sales area view`);
    if (await exists(driver, salesLinkSel, ELEMENT_TIMEOUT_MS)) {
      const salesLink = await driver.findElement(salesLinkSel);
      await scrollIntoView(driver, salesLink);
      console.log(`[Row ${row}] Found sales area`);

      console.log(`[Row ${row}] Waiting for SalesDetails`);
      const salesDetailsSel = By.css('#Sales');
      await driver.wait(until.elementLocated(salesDetailsSel), ELEMENT_TIMEOUT_MS);
      await driver.wait(until.elementIsVisible(await driver.findElement(salesDetailsSel)), ELEMENT_TIMEOUT_MS);

      // sold amount selector
      const soldAmountSel = By.css('#Sales > tbody > tr:nth-child(2) > td:nth-child(2)');
      const saleDateSel = By.css('#Sales > tbody > tr:nth-child(2) > td:nth-child(1)');

      try {
        if (await exists(driver, soldAmountSel, ELEMENT_TIMEOUT_MS)) {
          soldAmount = (await driver.findElement(soldAmountSel).getText()).trim();
        }
      } catch (e) {
        console.warn(`[Row ${row}] soldAmount extraction error: ${e.message}`);
      }

      try {
        if (await exists(driver, saleDateSel, ELEMENT_TIMEOUT_MS)) {
          saleDate = (await driver.findElement(saleDateSel).getText()).trim();
        }
      } catch (e) {
        console.warn(`[Row ${row}] saleDate extraction error: ${e.message}`);
      }

      console.log(`[Row ${row}] Extracted soldAmount: "${soldAmount}" saleDate: "${saleDate}"`);
    } else {
      console.log(`[Row ${row}] Sales link not found`);
    }
  } catch (e) {
    console.warn(`[Row ${row}] Sales extraction flow failed: ${e.message}`);
  }

  // Prepare batch write for only non-empty values
  const rowWrites = [];
  if (dorOwner) rowWrites.push({ range: dorOwnerA1, values: [[dorOwner]] });
  if (mailingAddress) rowWrites.push({ range: mailingAddrA1, values: [[mailingAddress]] });
  if (extraText) rowWrites.push({ range: extraFieldA1, values: [[extraText]] });
  if (soldAmount) rowWrites.push({ range: soldAmountA1, values: [[soldAmount]] });
  if (saleDate) rowWrites.push({ range: saleDateA1, values: [[saleDate]] });

  // final status marker if none already set
  const statusVal = 'processed';
  rowWrites.push({ range: statusA1, values: [[statusVal]] });

  if (rowWrites.length > 0) {
    try {
      await throttleWritesIfNeeded();
      await sheetsRequestWithRetries(() =>
        sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: {
            valueInputOption: 'USER_ENTERED',
            data: rowWrites,
          }
        })
      );
      console.log(`[Row ${row}] Batch wrote ${rowWrites.length} ranges`);
    } catch (e) {
      console.error(`[Row ${row}] Batch write failed: ${e.message}`);
      // attempt to write status with error message so row is marked and won't be retried immediately
      try {
        await throttleWritesIfNeeded();
        await sheetsRequestWithRetries(() =>
          sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: statusA1,
            valueInputOption: 'RAW',
            requestBody: { values: [[`error: ${String(e).slice(0,160)}`]] },
          })
        );
      } catch (ee) {
        console.error(`[Row ${row}] Failed to write error status: ${ee.message}`);
      }
    }
  } else {
    console.log(`[Row ${row}] Nothing to write for this row`);
  }

  console.log(`[Row ${row}] extractFromDetail: done`);
}

// -----------------------------
// Updated fetchDataAndUpdateSheet
// - Does not pre-write empty columns
// - Launches browser, runs existing extraction flows
// - Ensures all per-row A1 ranges are defined before use
// - Writes per-row with robust try/catch and compact logs
// -----------------------------
async function fetchDataAndUpdateSheet() {
  // define once, before processing rows
  const ranges = {
    dorOwnerPrefix: `${SHEET_NAME}!F`,
    saleDatePrefix: `${SHEET_NAME}!G`,
    soldAmountPrefix: `${SHEET_NAME}!H`,
    mailingAddrPrefix: `${SHEET_NAME}!I`,
    extraFieldPrefix: `${SHEET_NAME}!J`,
    statusPrefix: `${SHEET_NAME}!M`,
  };

  console.log('[Main] Starting fetchDataAndUpdateSheet');
  const sheets = await getSheetsClient();
  console.log('[Sheets] Fetching addresses, target URLs and statuses from sheet');

  const addressesRange = `${SHEET_NAME}!B${START_ROW}:B${END_ROW}`;
  const urlsRange = `${SHEET_NAME}!L${START_ROW}:L${END_ROW}`;
  const statusesRange = `${SHEET_NAME}!M${START_ROW}:M${END_ROW}`;

  const [addressesRes, urlsRes, statusesRes] = await Promise.all([
    sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: addressesRange }),
    sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: urlsRange }),
    sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: statusesRange }),
  ]);

  const addresses = (addressesRes.data.values || []).map(r => (r[0] || '').trim());
  const urls = (urlsRes.data.values || []).map(r => (r[0] || '').trim());
  const statuses = (statusesRes.data.values || []).map(r => (r[0] || '').trim());

  console.log(`[Init] Fetched ${addresses.length} addresses, ${urls.length} urls, ${statuses.length} status cells.`);

  const driver = await launchDriver();

  try {
    const rowsToProcess = Math.max(addresses.length, urls.length, statuses.length);
    for (let i = 0; i < rowsToProcess; i++) {
      const rowIndex = START_ROW + i;
      const targetUrl = urls[i] || '';
      const targetAddress = addresses[i] || '';
      const existingStatus = statuses[i] || '';

      console.log(`\n[Row ${rowIndex}] === START ===`);

      if (existingStatus) {
        console.log(`[Row ${rowIndex}] Status exists ("${existingStatus}") — skipping row.`);
        console.log(`[Row ${rowIndex}] === END ===\n`);
        continue;
      }

      if (!targetUrl) {
        console.log(`[Row ${rowIndex}] No URL found in sheet column L; skipping`);
        console.log(`[Row ${rowIndex}] === END ===\n`);
        continue;
      }

      console.log(`[Row ${rowIndex}] Navigating to URL: ${targetUrl}`);

      try {
        // navigate with timeout
        try {
          await Promise.race([
            driver.get(targetUrl),
            timeoutPromise(PAGE_LOAD_TIMEOUT_MS, `Page load timeout after ${PAGE_LOAD_TIMEOUT_MS}ms`)
          ]);
          console.log(`[Row ${rowIndex}] driver.get completed within ${PAGE_LOAD_TIMEOUT_MS}ms`);
        } catch (navErr) {
          console.warn(`[Row ${rowIndex}] Navigation warning: ${navErr.message}`);
          try { await driver.executeScript('if(window.stop) window.stop();'); console.log(`[Row ${rowIndex}] Invoked window.stop()`); } catch (e) { console.warn(`[Row ${rowIndex}] window.stop() failed: ${e.message}`); }
        }

        console.log(`[Row ${rowIndex}] Waiting briefly for DOM settlement`);
        await sleep(15000);

        // detect iframes and choose flow
        const iframes = await driver.findElements(By.css('iframe'));
        if (iframes.length > 0) {
          console.log(`[Row ${rowIndex}] Iframe(s) detected (${iframes.length}) -> treating as Detailed account`);
          try {
            await handleDetailedAccountByIframe(driver, rowIndex);
            const handles = await driver.getAllWindowHandles();
            if (handles.length > 1) {
              console.log(`[Row ${rowIndex}] Switching to newly opened tab for extraction`);
              await driver.switchTo().window(handles[handles.length - 1]);
              await sleep(500);
              await extractFromDetail(driver, sheets, i, ranges); // extractFromDetail expects zero-based index
              try { await driver.close(); console.log(`[Row ${rowIndex}] Closed detail tab`); } catch {}
              await driver.switchTo().window(handles[0]);
            } else {
              console.log(`[Row ${rowIndex}] No new tab opened; extracting on current page`);
              await extractFromDetail(driver, sheets, i, ranges);
            }
          } catch (e) {
            console.error(`[Row ${rowIndex}] Detailed flow error: ${e.stack || e.message}`);
            try {
              await throttleWritesIfNeeded();
              await sheetsRequestWithRetries(() =>
                sheets.spreadsheets.values.update({
                  spreadsheetId: SHEET_ID,
                  range: `${ranges.statusPrefix}${rowIndex}`,
                  valueInputOption: 'RAW',
                  requestBody: { values: [[`error: ${String(e).slice(0,200)}`]] },
                })
              );
            } catch {}
          }
        } else {
          console.log(`[Row ${rowIndex}] No iframe detected -> treating as Results list`);
          try {
            const result = await handleResultsAndMatch(driver, targetAddress, rowIndex);
            if (result && result.matched) {
              console.log(`[Row ${rowIndex}] Match clicked; handling post-click extraction`);
              await sleep(10000);
              const postIframes = await driver.findElements(By.css('iframe'));
              if (postIframes.length > 0) {
                console.log(`[Row ${rowIndex}] Iframe(s) detected after click (${postIframes.length}) -> treating as Detailed account`);
                try {
                  await handleDetailedAccountByIframe(driver, rowIndex);
                  const handles = await driver.getAllWindowHandles();
                  if (handles.length > 1) {
                    console.log(`[Row ${rowIndex}] Switching to newly opened tab for extraction`);
                    await driver.switchTo().window(handles[handles.length - 1]);
                    await sleep(500);
                    await extractFromDetail(driver, sheets, i, ranges);
                    try { await driver.close(); console.log(`[Row ${rowIndex}] Closed detail tab`); } catch {}
                    await driver.switchTo().window(handles[0]);
                  } else {
                    console.log(`[Row ${rowIndex}] No new tab opened; extracting on current page`);
                    await extractFromDetail(driver, sheets, i, ranges);
                  }
                } catch (e) {
                  console.error(`[Row ${rowIndex}] Detailed flow error: ${e.stack || e.message}`);
                  try {
                    await throttleWritesIfNeeded();
                    await sheetsRequestWithRetries(() =>
                      sheets.spreadsheets.values.update({
                        spreadsheetId: SHEET_ID,
                        range: `${ranges.statusPrefix}${rowIndex}`,
                        valueInputOption: 'RAW',
                        requestBody: { values: [[`error: ${String(e).slice(0,200)}`]] },
                      })
                    );
                  } catch {}
                }
              }
            } else {
              console.log(`[Row ${rowIndex}] No matched candidate found in results`);
              // handle explicit no-results element
              const noResultsXpath = By.xpath('//*[@id="index-search"]/div[1]/section/div[1]/div/div/div/div/div/p');
              if (await exists(driver, noResultsXpath, 2000)) {
                const txt = (await getTextSafe(driver, noResultsXpath)).toLowerCase();
                if (txt.includes('no result') || txt.includes('no results') || txt.includes('nothing found')) {
                  console.log(`[Row ${rowIndex}] Explicit no-results text found: "${txt}" -> marking no results`);
                  try {
                    await throttleWritesIfNeeded();
                    await sheetsRequestWithRetries(() =>
                      sheets.spreadsheets.values.update({
                        spreadsheetId: SHEET_ID,
                        range: `${ranges.statusPrefix}${rowIndex}`,
                        valueInputOption: 'RAW',
                        requestBody: { values: [['no results']] },
                      })
                    );
                  } catch {}
                } else {
                  console.log(`[Row ${rowIndex}] Results present but no match -> marking no match`);
                  try {
                    await throttleWritesIfNeeded();
                    await sheetsRequestWithRetries(() =>
                      sheets.spreadsheets.values.update({
                        spreadsheetId: SHEET_ID,
                        range: `${ranges.statusPrefix}${rowIndex}`,
                        valueInputOption: 'RAW',
                        requestBody: { values: [['no match']] },
                      })
                    );
                  } catch {}
                }
              } else {
                console.log(`[Row ${rowIndex}] No explicit no-results element; marking no match`);
                try {
                  await throttleWritesIfNeeded();
                  await sheetsRequestWithRetries(() =>
                    sheets.spreadsheets.values.update({
                      spreadsheetId: SHEET_ID,
                      range: `${ranges.statusPrefix}${rowIndex}`,
                      valueInputOption: 'RAW',
                      requestBody: { values: [['no match']] },
                    })
                  );
                } catch {}
              }
            }
          } catch (e) {
            console.error(`[Row ${rowIndex}] Results flow error: ${e.stack || e.message}`);
            try {
              await throttleWritesIfNeeded();
              await sheetsRequestWithRetries(() =>
                sheets.spreadsheets.values.update({
                  spreadsheetId: SHEET_ID,
                  range: `${ranges.statusPrefix}${rowIndex}`,
                  valueInputOption: 'RAW',
                  requestBody: { values: [[`error: ${String(e).slice(0,200)}`]] },
                })
              );
            } catch {}
          }
        }
      } catch (err) {
        console.error(`[Row ${rowIndex}] Navigation/processing error: ${err.stack || err.message}`);
        try {
          await throttleWritesIfNeeded();
          await sheetsRequestWithRetries(() =>
            sheets.spreadsheets.values.update({
              spreadsheetId: SHEET_ID,
              range: `${ranges.statusPrefix}${rowIndex}`,
              valueInputOption: 'RAW',
              requestBody: { values: [[`error: ${String(err).slice(0,200)}`]] },
            })
          );
        } catch {}
      } finally {
        // ensure no dangling detail tabs
        try {
          const handles = await driver.getAllWindowHandles();
          if (handles.length > 1) {
            for (let h = handles.length - 1; h > 0; h--) { try { await driver.switchTo().window(handles[h]); await driver.close(); } catch {} }
            await driver.switchTo().window(handles[0]);
          }
        } catch (e) {
          console.warn(`[Row ${rowIndex}] Tab cleanup warning: ${e.message}`);
        }
      }

      console.log(`[Row ${rowIndex}] === END ===\n`);
      await sleep(2000);
    }
  } finally {
    try {
      await driver.quit();
      console.log('[Browser] Driver quit');
    } catch (e) {
      console.warn('[Browser] Driver quit error:', e.message);
    }

    // Perform cleanup if available
    if (driver.cleanupUserDataDir) driver.cleanupUserDataDir();
  }

  console.log('[Main] fetchDataAndUpdateSheet completed');
}

// -----------------------------
// Entrypoint
// -----------------------------
(async () => {
  try {
    console.log('[Entrypoint] Starting leePA run');
    try { await makeRequestWithRetries('https://county-taxes.net', 2, 1000); } catch (e) { console.warn('[Entrypoint] Reachability quick-check failed:', e.message); }
    await fetchDataAndUpdateSheet();
    console.log('[Entrypoint] Completed leePA run');
  } catch (err) {
    console.error('[Fatal] Unhandled error:', err.stack || err.message);
    process.exit(1);
  }
})();
