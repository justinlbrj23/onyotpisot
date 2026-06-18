const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { wrapper } = require('axios-cookiejar-support');
const { CookieJar } = require('tough-cookie');
const he = require('he');
const { google } = require('googleapis');

const TARGET_URL = 'https://www.16thcircuit.org/browse-all-parcels';

const SHEET_ID = process.env.SHEET_ID || '1fdj-Lk5RIjuo4ekGiAHUPoW7JKqTuiy35b_Q8w2xTyg';
const SHEET_NAME = process.env.SHEET_NAME || 'Tax Sale Tracker';
const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), 'service-account.json');

function clean(value) {
  return String(value || '')
    .replace(/\u00A0/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function decodeHtml(value) {
  return clean(he.decode(value || ''));
}

function escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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

function parseAttributes(tag) {
  const attrs = {};
  const attrRegex = /([^\s=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g;
  let match;

  while ((match = attrRegex.exec(tag)) !== null) {
    const key = String(match[1] || '').toLowerCase();
    const value = match[2] ?? match[3] ?? match[4] ?? '';
    attrs[key] = value;
  }

  return attrs;
}

function extractInputs(html) {
  const inputs = [];
  const regex = /<input\b[^>]*>/gi;
  let match;

  while ((match = regex.exec(html)) !== null) {
    const tag = match[0];
    const attrs = parseAttributes(tag);

    inputs.push({
      raw: tag,
      type: (attrs.type || 'text').toLowerCase(),
      name: attrs.name || '',
      id: attrs.id || '',
      value: decodeHtml(attrs.value || ''),
      checked: Object.prototype.hasOwnProperty.call(attrs, 'checked')
    });
  }

  return inputs;
}

function extractTextareas(html) {
  const textareas = [];
  const regex = /<textarea\b([^>]*)>([\s\S]*?)<\/textarea>/gi;
  let match;

  while ((match = regex.exec(html)) !== null) {
    const attrs = parseAttributes(match[1] || '');
    textareas.push({
      name: attrs.name || '',
      id: attrs.id || '',
      value: decodeHtml(match[2] || '')
    });
  }

  return textareas;
}

function extractSelects(html) {
  const selects = [];
  const regex = /<select\b([^>]*)>([\s\S]*?)<\/select>/gi;
  let match;

  while ((match = regex.exec(html)) !== null) {
    const attrs = parseAttributes(match[1] || '');
    const inner = match[2] || '';

    let selectedValue = '';
    const optionRegex = /<option\b([^>]*)>([\s\S]*?)<\/option>/gi;
    let optionMatch;

    while ((optionMatch = optionRegex.exec(inner)) !== null) {
      const optionAttrs = parseAttributes(optionMatch[1] || '');
      const isSelected = Object.prototype.hasOwnProperty.call(optionAttrs, 'selected');
      const optionValue = decodeHtml(optionAttrs.value || optionMatch[2] || '');

      if (isSelected) {
        selectedValue = optionValue;
        break;
      }
    }

    if (!selectedValue) {
      optionRegex.lastIndex = 0;
      optionMatch = optionRegex.exec(inner);
      if (optionMatch) {
        const optionAttrs = parseAttributes(optionMatch[1] || '');
        selectedValue = decodeHtml(optionAttrs.value || optionMatch[2] || '');
      }
    }

    selects.push({
      name: attrs.name || '',
      id: attrs.id || '',
      value: selectedValue
    });
  }

  return selects;
}

function extractFormAction(html, baseUrl) {
  const match = html.match(/<form\b[^>]*action=(?:"([^"]*)"|'([^']*)'|([^\s>]+))[^>]*>/i);
  const action = decodeHtml(match?.[1] || match?.[2] || match?.[3] || '');
  return new URL(action || baseUrl, baseUrl).toString();
}

function getFormDataFromHtml(html) {
  const inputs = extractInputs(html);
  const textareas = extractTextareas(html);
  const selects = extractSelects(html);

  const data = {};

  for (const input of inputs) {
    if (!input.name) continue;

    if (input.type === 'checkbox' || input.type === 'radio') {
      if (input.checked) {
        data[input.name] = input.value || 'on';
      }
      continue;
    }

    if (['submit', 'button', 'image', 'reset', 'file'].includes(input.type)) {
      continue;
    }

    data[input.name] = input.value || '';
  }

  for (const ta of textareas) {
    if (ta.name) data[ta.name] = ta.value || '';
  }

  for (const sel of selects) {
    if (sel.name) data[sel.name] = sel.value || '';
  }

  return data;
}

function findSubmitByValue(html, submitValue) {
  const inputs = extractInputs(html);
  for (const input of inputs) {
    if (['submit', 'button'].includes(input.type) && clean(input.value).toLowerCase() === clean(submitValue).toLowerCase()) {
      return input;
    }
  }
  return null;
}

function getNextVisibleInputValuesAfterLabel(html, label, count = 1) {
  const labelRegex = new RegExp(escapeRegex(label).replace(/\s+/g, '\\s+'), 'i');
  const match = labelRegex.exec(html);

  if (!match) return [];

  const startIndex = match.index + match[0].length;
  const tail = html.slice(startIndex);

  const inputRegex = /<input\b[^>]*>/gi;
  const values = [];
  let inputMatch;

  while ((inputMatch = inputRegex.exec(tail)) !== null) {
    const tag = inputMatch[0];
    const attrs = parseAttributes(tag);

    const type = (attrs.type || 'text').toLowerCase();
    if (['hidden', 'submit', 'button', 'image', 'reset', 'file'].includes(type)) {
      continue;
    }

    const rawValue = decodeHtml(attrs.value || '');
    values.push(rawValue);

    if (values.length >= count) {
      break;
    }
  }

  return values;
}

function extractRecordFromHtml(html) {
  const suitNo = getNextVisibleInputValuesAfterLabel(html, 'Suit No', 1)[0] || '';
  const parcelNo = getNextVisibleInputValuesAfterLabel(html, 'Parcel No', 1)[0] || '';
  const owner = getNextVisibleInputValuesAfterLabel(html, 'Owner', 1)[0] || '';

  const propertyParts = getNextVisibleInputValuesAfterLabel(html, 'Property Address', 3);
  const propertyAddress = propertyParts.filter(Boolean).join(' ');

  const dateSold = getNextVisibleInputValuesAfterLabel(html, 'Date Sold', 1)[0] || '';
  const purchasePrice = getNextVisibleInputValuesAfterLabel(html, 'Purchase Price', 1)[0] || '';
  const judgment = getNextVisibleInputValuesAfterLabel(html, 'Judgment', 1)[0] || '';
  const excess = getNextVisibleInputValuesAfterLabel(html, 'Excess', 1)[0] || '';
  const purchaser = getNextVisibleInputValuesAfterLabel(html, 'Purchaser', 1)[0] || '';

  return {
    suitNo: clean(suitNo),
    parcelNo: clean(parcelNo),
    owner: clean(owner),
    propertyAddress: clean(propertyAddress),
    dateSold: clean(dateSold),
    purchasePrice: clean(purchasePrice),
    judgment: clean(judgment),
    excess: clean(excess),
    purchaser: clean(purchaser)
  };
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
  const signatures = new Set();

  for (const row of rows) {
    const record = {
      propertyAddress: row[0] || '',
      owner: row[1] || '',
      dateSold: row[3] || '',
      judgment: row[4] || '',
      purchasePrice: row[5] || '',
      excess: row[6] || '',
      purchaser: row[7] || ''
    };

    signatures.add(makeSignature(record));
  }

  return signatures;
}

async function appendRowsToSheet(sheets, records) {
  if (!records.length) {
    console.log('No new rows to append.');
    return;
  }

  const values = records.map((r) => [
    clean(r.propertyAddress), // B
    clean(r.owner),           // C
    '',                       // D blank
    clean(r.dateSold),        // E
    clean(r.judgment),        // F
    clean(r.purchasePrice),   // G
    clean(r.excess),          // H
    clean(r.purchaser)        // I
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!B2:I`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });

  console.log(`Appended ${values.length} new row(s) to Google Sheets.`);
}

async function createHttpClient() {
  const jar = new CookieJar();

  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    headers: {
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    },
    timeout: 60000,
    maxRedirects: 5
  }));

  return client;
}

async function fetchInitialPage(client) {
  const resp = await client.get(TARGET_URL);
  return {
    url: resp.request?.res?.responseUrl || TARGET_URL,
    html: String(resp.data || '')
  };
}

async function postNext(client, currentUrl, html) {
  const actionUrl = extractFormAction(html, currentUrl);
  const formData = getFormDataFromHtml(html);
  const nextButton = findSubmitByValue(html, 'Next');

  if (!nextButton || !nextButton.name) {
    return null;
  }

  formData[nextButton.name] = nextButton.value || 'Next';

  const body = new URLSearchParams();
  for (const [key, value] of Object.entries(formData)) {
    body.append(key, value ?? '');
  }

  const resp = await client.post(actionUrl, body.toString(), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': currentUrl
    }
  });

  return {
    url: resp.request?.res?.responseUrl || actionUrl,
    html: String(resp.data || '')
  };
}

async function scrapeAllParcels() {
  const client = await createHttpClient();
  let { url: currentUrl, html } = await fetchInitialPage(client);

  const records = [];
  const seenPageHashes = new Set();
  const seenRecordSignatures = new Set();

  const MAX_PAGES = Number(process.env.MAX_PAGES || 5000);
  let count = 0;

  while (count < MAX_PAGES) {
    count++;

    const pageHash = clean(html).slice(0, 5000);
    if (seenPageHashes.has(pageHash)) {
      console.log('Detected repeated HTML page. Stopping.');
      break;
    }
    seenPageHashes.add(pageHash);

    const record = extractRecordFromHtml(html);

    console.log('Extracted record:', JSON.stringify(record, null, 2));

    if (isRecordUsable(record)) {
      const sig = makeSignature(record);

      if (!seenRecordSignatures.has(sig)) {
        seenRecordSignatures.add(sig);
        records.push(record);

        console.log(
          `[${records.length}] ` +
          `Suit No="${record.suitNo}" | ` +
          `Parcel No="${record.parcelNo}" | ` +
          `Property Address="${record.propertyAddress}" | ` +
          `Owner="${record.owner}" | ` +
          `Date Sold="${record.dateSold}" | ` +
          `Judgment="${record.judgment}" | ` +
          `Purchase Price="${record.purchasePrice}" | ` +
          `Excess="${record.excess}" | ` +
          `Purchaser="${record.purchaser}"`
        );
      } else {
        console.log('Duplicate record within current run; skipping.');
      }
    } else {
      console.log('Record had no usable data; skipping current page.');
    }

    const nextPage = await postNext(client, currentUrl, html);

    if (!nextPage) {
      console.log('No Next submit button found in raw HTML. Stopping.');
      break;
    }

    const nextRecord = extractRecordFromHtml(nextPage.html);
    const currentSig = makeSignature(record);
    const nextSig = makeSignature(nextRecord);

    if (nextSig && nextSig === currentSig) {
      console.log('Next page returned same record. Assuming end of records.');
      break;
    }

    currentUrl = nextPage.url;
    html = nextPage.html;
  }

  console.log(`Scraping complete. Collected ${records.length} unique record(s).`);
  return records;
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
