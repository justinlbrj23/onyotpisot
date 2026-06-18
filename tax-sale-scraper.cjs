const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const { google } = require('googleapis');

const TARGET_URL = 'https://www.16thcircuit.org/browse-all-parcels';
const PANEL_SELECTOR = 'div.panelwrapper';

const SHEET_ID = process.env.SHEET_ID || '1fdj-Lk5RIjuo4ekGiAHUPoW7JKqTuiy35b_Q8w2xTyg';
const SHEET_NAME = process.env.SHEET_NAME || 'Tax Sale Tracker';
const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), 'service-account.json');

function clean(value) {
  return String(value || '')
    .replace(/\u00A0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeMoney(value) {
  const v = clean(value);
  return v || '';
}

function normalizeDate(value) {
  const v = clean(value);
  return v || '';
}

function makeSignature(record) {
  return [
    clean(record.propertyAddress).toUpperCase(),
    clean(record.owner).toUpperCase(),
    clean(record.dateSold).toUpperCase(),
    clean(record.judgment).toUpperCase(),
    clean(record.purchasePrice).toUpperCase(),
    clean(record.excess).toUpperCase(),
    clean(record.purchaser).toUpperCase()
  ].join(' | ');
}

async function getGoogleSheetsClient() {
  if (!fs.existsSync(SERVICE_ACCOUNT_FILE)) {
    throw new Error(`Missing service account file: ${SERVICE_ACCOUNT_FILE}`);
  }

  const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_FILE,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  return google.sheets({
    version: 'v4',
    auth
  });
}

async function getExistingSignatures(sheets) {
  const range = `${SHEET_NAME}!B2:I`;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });

  const rows = resp.data.values || [];
  const set = new Set();

  for (const row of rows) {
    // B:I mapping
    // [B, C, D, E, F, G, H, I]
    const record = {
      propertyAddress: row[0] || '',
      owner: row[1] || '',
      dateSold: row[3] || '',
      judgment: row[4] || '',
      purchasePrice: row[5] || '',
      excess: row[6] || '',
      purchaser: row[7] || ''
    };
    set.add(makeSignature(record));
  }

  return set;
}

async function appendRowsToSheet(sheets, records) {
  if (!records.length) {
    console.log('No new rows to append.');
    return;
  }

  const values = records.map((r) => [
    clean(r.propertyAddress), // B
    clean(r.owner),           // C
    '',                       // D intentionally blank
    normalizeDate(r.dateSold),// E
    normalizeMoney(r.judgment),      // F
    normalizeMoney(r.purchasePrice), // G
    normalizeMoney(r.excess),        // H
    clean(r.purchaser)        // I
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!B2:I`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: {
      values
    }
  });

  console.log(`Appended ${values.length} new row(s) to Google Sheets.`);
}

async function extractRecordFromPanel(page) {
  return await page.locator(PANEL_SELECTOR).evaluate((panel) => {
    function clean(value) {
      return String(value || '')
        .replace(/\u00A0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    }

    function extractValue(node) {
      if (!node) return '';

      // If the node itself is a form field
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(node.tagName)) {
        const val = clean(node.value || node.textContent || '');
        if (val) return val;
      }

      // Look for non-empty form field descendants first
      const field = node.querySelector('input, textarea, select');
      if (field) {
        const val = clean(field.value || field.textContent || '');
        if (val) return val;
      }

      // Then try plain text (excluding nested labels where possible)
      const txt = clean(node.textContent || '');
      return txt;
    }

    function getCellLikeElements(root) {
      return Array.from(root.querySelectorAll('td, th, div, span, label'));
    }

    function matchesLabel(el, label) {
      return clean(el.textContent) === label;
    }

    function findValueForLabel(label) {
      const candidates = getCellLikeElements(panel).filter((el) => matchesLabel(el, label));

      for (const labelEl of candidates) {
        // Strategy 1: same table row, next cells
        const row = labelEl.closest('tr');
        if (row) {
          const cells = Array.from(row.querySelectorAll('td, th'));
          const idx = cells.findIndex((c) => c === labelEl || c.contains(labelEl));
          if (idx >= 0) {
            for (let i = idx + 1; i < cells.length; i++) {
              const val = extractValue(cells[i]);
              if (val) return val;
            }
          }
        }

        // Strategy 2: parent cell next sibling
        const immediateContainer = labelEl.closest('td, th, div, span, label') || labelEl;
        let next = immediateContainer.nextElementSibling;
        while (next) {
          const val = extractValue(next);
          if (val) return val;
          next = next.nextElementSibling;
        }

        // Strategy 3: walk up a few levels and inspect next siblings
        let current = labelEl;
        for (let depth = 0; depth < 4 && current; depth++) {
          let sib = current.nextElementSibling;
          while (sib) {
            const val = extractValue(sib);
            if (val) return val;
            sib = sib.nextElementSibling;
          }
          current = current.parentElement;
        }
      }

      return '';
    }

    return {
      suitNo: findValueForLabel('Suit No'),
      parcelNo: findValueForLabel('Parcel No'),
      propertyAddress: findValueForLabel('Property Address'),
      owner: findValueForLabel('Owner'),
      dateSold: findValueForLabel('Date Sold'),
      judgment: findValueForLabel('Judgment'),
      purchasePrice: findValueForLabel('Purchase Price'),
      excess: findValueForLabel('Excess'),
      purchaser: findValueForLabel('Purchaser')
    };
  });
}

function isRecordUsable(record) {
  return Boolean(
    clean(record.propertyAddress) ||
    clean(record.owner) ||
    clean(record.dateSold) ||
    clean(record.judgment) ||
    clean(record.purchasePrice) ||
    clean(record.excess) ||
    clean(record.purchaser)
  );
}

async function clickNextAndWaitForChange(page, beforeFingerprint) {
  const nextLocator = page.locator('input[value="Next"], button:has-text("Next"), text=Next').first();

  if (!(await nextLocator.count())) {
    console.log('No Next button found. Stopping.');
    return false;
  }

  try {
    await nextLocator.scrollIntoViewIfNeeded();
  } catch (_) {}

  const isDisabled = await nextLocator.evaluate((el) => {
    const disabledAttr = el.getAttribute('disabled');
    const ariaDisabled = el.getAttribute('aria-disabled');
    return el.disabled === true || disabledAttr !== null || ariaDisabled === 'true';
  }).catch(() => false);

  if (isDisabled) {
    console.log('Next button is disabled. Stopping.');
    return false;
  }

  try {
    await Promise.all([
      page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => null),
      nextLocator.click({ timeout: 10000 })
    ]);
  } catch (err) {
    console.log(`Failed clicking Next: ${err.message}`);
    return false;
  }

  try {
    await page.waitForSelector(PANEL_SELECTOR, { state: 'visible', timeout: 15000 });
  } catch (_) {}

  // Wait until the panel changes
  try {
    await page.waitForFunction(
      ({ selector, before }) => {
        const el = document.querySelector(selector);
        if (!el) return false;
        const now = (el.innerText || '').replace(/\s+/g, ' ').trim();
        return now !== before;
      },
      { selector: PANEL_SELECTOR, before: beforeFingerprint },
      { timeout: 15000 }
    );
  } catch (_) {
    // If it did not change, likely at the end
    const afterPanelText = await page.locator(PANEL_SELECTOR).innerText().catch(() => '');
    const afterFingerprint = clean(afterPanelText);
    if (afterFingerprint === beforeFingerprint) {
      console.log('Panel did not change after Next. Assuming end of records.');
      return false;
    }
  }

  return true;
}

async function scrapeAllParcels() {
  const browser = await chromium.launch({
    headless: true
  });

  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 }
  });

  const records = [];
  const seenPageFingerprints = new Set();
  const seenRecordSignatures = new Set();

  try {
    console.log(`Opening: ${TARGET_URL}`);
    await page.goto(TARGET_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.waitForSelector(PANEL_SELECTOR, {
      state: 'visible',
      timeout: 60000
    });

    let safetyCounter = 0;
    const MAX_PAGES = Number(process.env.MAX_PAGES || 5000);

    while (safetyCounter < MAX_PAGES) {
      safetyCounter++;

      const panelText = clean(await page.locator(PANEL_SELECTOR).innerText().catch(() => ''));
      if (!panelText) {
        console.log('Empty panel text encountered. Stopping.');
        break;
      }

      if (seenPageFingerprints.has(panelText)) {
        console.log('Detected repeated page fingerprint. Stopping.');
        break;
      }
      seenPageFingerprints.add(panelText);

      const record = await extractRecordFromPanel(page);

      if (isRecordUsable(record)) {
        const signature = makeSignature(record);
        if (!seenRecordSignatures.has(signature)) {
          seenRecordSignatures.add(signature);
          records.push(record);

          console.log(
            `[${records.length}] ` +
            `Property Address="${record.propertyAddress}" | ` +
            `Owner="${record.owner}" | ` +
            `Date Sold="${record.dateSold}" | ` +
            `Judgment="${record.judgment}" | ` +
            `Purchase Price="${record.purchasePrice}" | ` +
            `Excess="${record.excess}" | ` +
            `Purchaser="${record.purchaser}"`
          );
        } else {
          console.log('Duplicate record detected in session; skipping.');
        }
      } else {
        console.log('Record had no usable data; skipping.');
      }

      const moved = await clickNextAndWaitForChange(page, panelText);
      if (!moved) {
        break;
      }
    }

    console.log(`Scraping complete. Collected ${records.length} unique record(s).`);
    return records;
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function main() {
  console.log('Starting scraper...');

  const records = await scrapeAllParcels();

  if (!records.length) {
    console.log('No records scraped. Exiting without Sheets update.');
    return;
  }

  const sheets = await getGoogleSheetsClient();
  const existingSignatures = await getExistingSignatures(sheets);

  const newRecords = records.filter((r) => !existingSignatures.has(makeSignature(r)));

  console.log(`Existing sheet signatures: ${existingSignatures.size}`);
  console.log(`New records to append: ${newRecords.length}`);

  await appendRowsToSheet(sheets, newRecords);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
``
