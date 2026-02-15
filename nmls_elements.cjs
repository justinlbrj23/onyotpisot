// Requires:
// npm install puppeteer-real-browser cheerio googleapis

const { connect } = require('puppeteer-real-browser');
const cheerio = require('cheerio');
const { google } = require('googleapis');
const fs = require('fs').promises;

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

const sheets = google.sheets({
  version: 'v4',
  auth,
});

// =========================
// Helper: sleep (replacement for page.waitForTimeout)
// =========================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// =========================
// FUNCTION: Inspect Web Page
// =========================

async function inspectPage(url) {
  let browser;
  try {
    const connection = await connect({
      headless: false,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      customConfig: {},
      turnstile: true,
      connectOption: {},
      disableXvfb: false,
    });

    browser = connection.browser;
    const page = connection.page;

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
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// FUNCTION: Perform Search (robust, race-free)
// =========================

async function searchPage(url, zipcode) {
  let browser;
  try {
    const connection = await connect({
      headless: false,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      customConfig: {},
      turnstile: true,
      connectOption: {},
      disableXvfb: false,
    });

    browser = connection.browser;
    const page = connection.page;

    console.log("🌐 Navigating...");
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    await page.waitForSelector('body', { timeout: 15000 });

    console.log("📥 Collecting input candidates from live DOM...");
    const inputs = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('input')).map(i => ({
        id: i.id || null,
        name: i.name || null,
        type: i.type || null,
        placeholder: i.placeholder || null,
        class: i.className || null,
      }));
    });
    console.log("🔎 Candidate inputs:", inputs);

    // Heuristic: choose a text input with search/zip/postal in id/name/placeholder/class
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
      // fallback: pick the first visible text input
      const fallback = await page.evaluate(() => {
        const el = Array.from(document.querySelectorAll('input')).find(i => (i.type === 'text' || i.type === 'search' || !i.type) && i.offsetParent !== null);
        if (!el) return null;
        if (el.id) return `#${el.id}`;
        if (el.name) return `[name="${el.name}"]`;
        if (el.placeholder) return `input[placeholder="${el.placeholder}"]`;
        if (el.className) return `input.${el.className.trim().split(/\s+/).join('.')}`;
        return null;
      });
      targetSelector = fallback;
    }

    if (!targetSelector) {
      throw new Error("Could not auto-detect search input field");
    }

    console.log(`🎯 Using input selector: ${targetSelector}`);
    await page.waitForSelector(targetSelector, { timeout: 15000 });

    // Clear and type the ZIP
    await page.evaluate(sel => {
      const el = document.querySelector(sel);
      if (el) { el.value = ''; el.focus(); }
    }, targetSelector);

    console.log("⌨️ Typing ZIP and submitting...");
    await page.type(targetSelector, zipcode);

    // Determine a likely results container selector (best-effort)
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

    // Submit and wait for either navigation or the results container to appear/refresh
    const navPromise = page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 }).catch(() => null);
    const pressPromise = page.keyboard.press('Enter');
    await pressPromise;

    // Wait for either navigation or results container
    if (likelyResultsSelector) {
      await page.waitForSelector(likelyResultsSelector, { timeout: 15000 }).catch(() => null);
    } else {
      // fallback: wait a short time for dynamic content to render
      await Promise.race([navPromise, new Promise(res => setTimeout(res, 1200))]);
    }

    // Give the page a short moment to finish rendering dynamic results
    await sleep(800);

    // Extract results inside the page to avoid content() race conditions
    const results = await page.evaluate(() => {
      const out = [];
      // Prefer containers that look like results
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
        // parse first container with rows
        for (const c of containers) {
          // FIXED: use Array.from on querySelectorAll (previously had a typo)
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
        // fallback: try to find list items that look like results
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
    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// FUNCTION: Append to Google Sheets (generic)
// =========================

async function appendToSheet(results) {
  if (!results || results.length === 0) {
    console.warn('⚠️ No data to write.');
    return;
  }

  const timestamp = new Date().toISOString();

  // Detect shape: if objects have attrs/tag/text use that; otherwise treat as name/details
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
    // fallback: stringify
    values = results.map(r => [timestamp, JSON.stringify(r)]);
  }

  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    if (!existing.data.values || existing.data.values.length === 0) {
      // Determine header based on shape
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