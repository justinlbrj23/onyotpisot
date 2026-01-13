// inspectWebpage.cjs
// SOLD + SURPLUS extractor (JS-pagination aware)

const puppeteer = require("puppeteer");
const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "web_tda";
const URL_RANGE = "C2:C";

const OUTPUT_ROWS_FILE = "parsed-auctions.json";
const OUTPUT_ERRORS_FILE = "errors.json";

const MIN_SURPLUS = 25000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// LOAD URLS
// =========================
async function loadUrls() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  return (res.data.values || [])
    .flat()
    .map(v => v.trim())
    .filter(v => v.startsWith("http"));
}

// =========================
// HELPERS
// =========================
function money(v) {
  if (!v) return null;
  const n = parseFloat(v.replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// SCRAPE ONE URL (ALL PAGES)
// =========================
async function scrapeUrl(browser, baseUrl) {
  const page = await browser.newPage();
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120"
  );

  console.log(`🌐 Scraping ${baseUrl}`);
  await page.goto(baseUrl, { waitUntil: "networkidle2", timeout: 120000 });
  await page.waitForTimeout(5000);

  const results = [];
  let pageNum = 1;

  while (true) {
    console.log(`➡️ Page ${pageNum}`);

    const pageRows = await page.evaluate(
      ({ MIN_SURPLUS }) => {
        const rows = [];

        document.querySelectorAll(".AuctionSold").forEach(badge => {
          const card = badge.closest(".auctionItem") || badge.parentElement;
          if (!card) return;

          const get = label => {
            const el = [...card.querySelectorAll("*")].find(e =>
              e.textContent.includes(label)
            );
            return el?.nextElementSibling?.textContent?.trim() || "";
          };

          const sale = parseFloat(
            get("Amount").replace(/[^0-9.-]/g, "")
          );
          const assessed = parseFloat(
            get("Assessed Value").replace(/[^0-9.-]/g, "")
          );

          if (!sale || !assessed) return;
          const surplus = assessed - sale;
          if (surplus < MIN_SURPLUS) return;

          rows.push({
            sourceUrl: location.href,
            auctionStatus: "Sold",
            auctionType: get("Auction Type") || "Tax Sale",
            caseNumber: get("Case #"),
            parcelId: get("Parcel").split("|")[0].trim(),
            propertyAddress: get("Property Address"),
            salePrice: sale,
            assessedValue: assessed,
            surplus,
            meetsMinimumSurplus: "Yes",
          });
        });

        return rows;
      },
      { MIN_SURPLUS }
    );

    results.push(...pageRows);

    const nextExists = await page.$(
      'a[title*="Next"], a:has(img[alt="Next"])'
    );
    if (!nextExists) break;

    await Promise.all([
      page.click('a[title*="Next"], a:has(img[alt="Next"])'),
      page.waitForNavigation({ waitUntil: "networkidle2" }),
    ]);

    await page.waitForTimeout(4000);
    pageNum++;
  }

  await page.close();
  return results;
}

// =========================
// MAIN
// =========================
(async () => {
  const urls = await loadUrls();
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox"],
  });

  const all = [];
  const seen = new Set();
  const errors = [];

  for (const url of urls) {
    try {
      const rows = await scrapeUrl(browser, url);
      for (const r of rows) {
        const k = `${r.caseNumber}|${r.parcelId}`;
        if (!seen.has(k)) {
          seen.add(k);
          all.push(r);
        }
      }
    } catch (e) {
      errors.push({ url, error: e.message });
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(all, null, 2));
  if (errors.length)
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log(`✅ Saved ${all.length} SOLD + SURPLUS auctions`);
})();