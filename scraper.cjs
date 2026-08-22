const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');
const pdfParse = require('pdf-parse');

const SHEET_ID =
  process.env.SHEET_ID ||
  '1iXOEgk_jYxeykjXafgCSc80KkAn-Jp2jl27ZS7FqGdg';

const WORKSHEET_NAME =
  process.env.WORKSHEET_NAME ||
  'Notice of LP';

const SERVICE_ACCOUNT_FILE =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  'service-account.json';

const START_ROW = Number(process.env.START_ROW) || 2;
const PDF_TIMEOUT_MS = Number(process.env.PDF_TIMEOUT_MS) || 120000;
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS) || 1500;
const OVERWRITE_EXISTING = process.env.OVERWRITE_EXISTING === 'true';
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;

const OUTPUT_JSON = 'pdf-extraction-results.json';
const FAILURE_JSON = 'pdf-extraction-failures.json';

function cleanText(value = '') {
  return String(value)
    .replace(/\u00a0/g, ' ')
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/\r/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function flattenText(value = '') {
  return cleanText(value)
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeUrl(value = '') {
  const url = String(value).trim();

  if (!url) {
    return '';
  }

  if (!/^https?:\/\//i.test(url)) {
    throw new Error(`Column D value is not an HTTP(S) URL: ${url}`);
  }

  return url;
}

function extractCaseOrInstrumentNumber(text = '') {
  const normalized = flattenText(text);

  /*
   * Collect every Instrument No./Number match and use the final match.
   * Trustee notices often mention the original recording first and the
   * assignment instrument later. The supplied example targets the later
   * value: 2026-002285.
   */
  const instrumentPatterns = [
    /\bInstrument\s+No\.?\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/gi,
    /\bInstrument\s+Number\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/gi
  ];

  const matches = [];

  for (const pattern of instrumentPatterns) {
    for (const match of normalized.matchAll(pattern)) {
      if (match[1]) {
        matches.push({
          value: match[1],
          index: match.index ?? -1
        });
      }
    }
  }

  if (matches.length > 0) {
    matches.sort((left, right) => left.index - right.index);
    return matches.at(-1).value
      .replace(/[.,;:]+$/g, '')
      .toUpperCase();
  }

  /* Fallbacks when a notice uses case or T.S. numbering instead. */
  const fallbackPatterns = [
    /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bTS\s+No\.?\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
  ];

  for (const pattern of fallbackPatterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) {
      return match[1].replace(/[.,;:]+$/g, '').toUpperCase();
    }
  }

  return '';
}

function extractDefendant(text = '') {
  const normalized = flattenText(text);

  /*
   * Capture any variable-length content located between "made by," and
   * "as Grantor". New lines and inconsistent spacing are already flattened.
   */
  const patterns = [
    /\bmade\s+by\s*,\s*(.+?)\s+as\s+Grantor\b/i,
    /\bmade\s+by\s+(.+?)\s+as\s+Grantor\b/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) {
      return match[1]
        .replace(/^[,;:\-\s]+|[,;:\-\s]+$/g, '')
        .replace(/\s+/g, ' ')
        .trim();
    }
  }

  return '';
}

function extractSubjectProperty(text = '') {
  const normalized = flattenText(text);

  /*
   * Capture text after "Commonly known as:" and stop at a five-digit ZIP.
   * An optional ZIP+4 suffix is retained when present.
   */
  const match = normalized.match(
    /\bCommonly\s+known\s+as\s*:\s*(.+?\b\d{5}(?:-\d{4})?)(?=\s|$)/i
  );

  if (!match?.[1]) {
    return '';
  }

  return match[1]
    .replace(/^[,;:\-\s]+|[,;:\-\s]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTargets(text = '') {
  return {
    caseOrInstrumentNumber: extractCaseOrInstrumentNumber(text),
    defendant: extractDefendant(text),
    subjectProperty: extractSubjectProperty(text)
  };
}

function escapeSheetName(name) {
  return `'${String(name).replace(/'/g, "''")}'`;
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function fetchPdf(url, attempt = 1) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), PDF_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'User-Agent':
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
          '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        Accept: 'application/pdf,application/octet-stream;q=0.9,*/*;q=0.8'
      }
    });

    if (!response.ok) {
      throw new Error(
        `PDF request failed with HTTP ${response.status} ${response.statusText}`
      );
    }

    const contentType = response.headers.get('content-type') || '';
    const buffer = Buffer.from(await response.arrayBuffer());

    if (buffer.length < 5 || buffer.subarray(0, 5).toString() !== '%PDF-') {
      throw new Error(
        `URL did not return a valid PDF. Content-Type: ${contentType || 'unknown'}`
      );
    }

    return buffer;
  } catch (error) {
    if (attempt >= MAX_RETRIES) {
      throw error;
    }

    const retryDelay = Math.min(2000 * (2 ** (attempt - 1)), 15000);
    console.warn(
      `Attempt ${attempt} failed for ${url}: ${error.message}. ` +
      `Retrying in ${retryDelay}ms...`
    );

    await sleep(retryDelay);
    return fetchPdf(url, attempt + 1);
  } finally {
    clearTimeout(timeout);
  }
}

async function parsePdf(buffer) {
  const result = await pdfParse(buffer, {
    pagerender: undefined,
    max: 0,
    version: 'default'
  });

  const text = cleanText(result.text || '');

  if (!text) {
    throw new Error(
      'No embedded text was found in the PDF. The file may require OCR.'
    );
  }

  return {
    text,
    pageCount: result.numpages || 0,
    metadata: result.info || {}
  };
}

async function createSheetsClient() {
  if (!fs.existsSync(SERVICE_ACCOUNT_FILE)) {
    throw new Error(`Service account file not found: ${SERVICE_ACCOUNT_FILE}`);
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

async function readSheetRows(sheets) {
  const sheet = escapeSheetName(WORKSHEET_NAME);
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${sheet}!D${START_ROW}:G`
  });

  return response.data.values || [];
}

async function writeExtractedTargets(sheets, rowNumber, targets) {
  const sheet = escapeSheetName(WORKSHEET_NAME);

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${sheet}!E${rowNumber}:G${rowNumber}`,
    valueInputOption: 'RAW',
    requestBody: {
      values: [[
        targets.caseOrInstrumentNumber,
        targets.defendant,
        targets.subjectProperty
      ]]
    }
  });
}

async function processRows() {
  const sheets = await createSheetsClient();
  const rows = await readSheetRows(sheets);
  const results = [];
  const failures = [];

  console.log(
    `Loaded ${rows.length} row(s) from ` +
    `${WORKSHEET_NAME}!D${START_ROW}:G.`
  );

  for (let index = 0; index < rows.length; index += 1) {
    const rowNumber = START_ROW + index;
    const row = rows[index] || [];
    const urlValue = row[0] || '';
    const existingE = row[1] || '';
    const existingF = row[2] || '';
    const existingG = row[3] || '';

    if (!String(urlValue).trim()) {
      console.log(`Row ${rowNumber}: column D is blank; skipping.`);
      continue;
    }

    if (
      !OVERWRITE_EXISTING &&
      String(existingE).trim() &&
      String(existingF).trim() &&
      String(existingG).trim()
    ) {
      console.log(`Row ${rowNumber}: E:G already populated; skipping.`);
      continue;
    }

    try {
      const url = normalizeUrl(urlValue);
      console.log(`Row ${rowNumber}: fetching PDF ${url}`);

      const buffer = await fetchPdf(url);
      const parsed = await parsePdf(buffer);
      const targets = extractTargets(parsed.text);

      await writeExtractedTargets(sheets, rowNumber, targets);

      const missing = Object.entries(targets)
        .filter(([, value]) => !value)
        .map(([key]) => key);

      const record = {
        row: rowNumber,
        url,
        pageCount: parsed.pageCount,
        ...targets,
        missingTargets: missing
      };

      results.push(record);

      console.log(
        `Row ${rowNumber}: wrote E:G -> ` +
        `${targets.caseOrInstrumentNumber || '[blank]'} | ` +
        `${targets.defendant || '[blank]'} | ` +
        `${targets.subjectProperty || '[blank]'}`
      );

      fs.writeFileSync(OUTPUT_JSON, JSON.stringify(results, null, 2), 'utf8');
    } catch (error) {
      const failure = {
        row: rowNumber,
        url: String(urlValue).trim(),
        error: error.message
      };

      failures.push(failure);
      fs.writeFileSync(FAILURE_JSON, JSON.stringify(failures, null, 2), 'utf8');
      console.error(`Row ${rowNumber} failed: ${error.message}`);
    }

    if (REQUEST_DELAY_MS > 0 && index < rows.length - 1) {
      await sleep(REQUEST_DELAY_MS);
    }
  }

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(results, null, 2), 'utf8');
  fs.writeFileSync(FAILURE_JSON, JSON.stringify(failures, null, 2), 'utf8');

  console.log('====================================');
  console.log(`Successful rows: ${results.length}`);
  console.log(`Failed rows: ${failures.length}`);
  console.log('====================================');

  if (failures.length > 0) {
    process.exitCode = 1;
  }
}

processRows().catch(error => {
  console.error('PDF sheet processor failed:', error);
  process.exitCode = 1;
});
