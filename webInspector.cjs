// webInspector.cjs
// RealForeclose SOLD auction parser (JS-rendered, click-pagination SAFE)

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
const MAX_PAGES_SAFETY = 25;

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
async function loadTargetUrls() {
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
function money(val) {
  if (!val) return null;
  const n = parseFloat(val.replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// MAIN PARSER (LIVE DOM)
// =========================
async function scrapeAuction(browser, startUrl) {
  const page = await browser.newPage();
  const rows = [];
  let pageNum = 1;

  try {
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    );

    console.log(`🌐 Visiting ${startUrl}`);
    await page.goto(startUrl, { waitUntil: "networkidle2" });

    while (pageNum <= MAX_PAGES_SAFETY) {
      console.log(`➡️ Scanning page ${pageNum}`);

      // wait for auction cards
      await page.waitForSelector(".auctionItem", { timeout: 15000 });

      const pageRows = await page.evaluate(
        ({ MIN_SURPLUS, startUrl }) => {
          const out = [];

          document.querySelectorAll(".auctionItem").forEach(card => {
            // SOLD badge check (LIVE DOM)
            if (!card.innerText.includes("SOLD")) return;

            function grab(label) {
              const el = [...card.querySelectorAll("td, div")]
                .find(n => n.textContent.includes(label));
              return el?.nextElementSibling?.textContent?.trim() || "";
            }

            const sale = grab("Amount");
            const assessed = grab("Assessed Value");

            const saleNum = parseFloat(sale.replace(/[^0-9.-]/g, ""));
            const assessedNum = parseFloat(assessed.replace(/[^0-9.-]/g, ""));

            if (!saleNum || !assessedNum) return;

            const surplus = assessedNum - saleNum;
            if (surplus < MIN_SURPLUS) return;

            out.push({
              sourceUrl: startUrl,
              auctionStatus: "Sold",
              auctionType: grab("Auction Type") || "Tax Sale",
              caseNumber: grab("Case #"),
              parcelId: grab("Parcel ID").split("|")[0].trim(),
              propertyAddress: grab("Property Address"),
              salePrice: saleNum,
              assessedValue: assessedNum,
              surplus,
              meetsMinimumSurplus: "Yes",
            });
          });

          return out;
        },
        { MIN_SURPLUS, startUrl }
      );

      rows.push(...pageRows);

      // Try clicking NEXT
      const nextBtn = await page.$("a[onclick*='next']");
      if (!nextBtn) break;

      await Promise.all([
        nextBtn.click(),
        page.waitForNavigation({ waitUntil: "networkidle2" }),
      ]);

      pageNum++;
    }

  } catch (err) {
    return { rows, error: err.message };
  } finally {
    await page.close();
  }

  return { rows };
}

// =========================
// MAIN
// =========================
(async () => {
  const urls = await loadTargetUrls();
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox"],
  });

  const all = [];
  const seen = new Set();
  const errors = [];

  for (const url of urls) {
    const res = await scrapeAuction(browser, url);

    for (const r of res.rows) {
      const key = `${r.caseNumber}|${r.parcelId}`;
      if (!seen.has(key)) {
        seen.add(key);
        all.push(r);
      }
    }

    if (res.error) errors.push(res.error);
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(all, null, 2));
  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
  }

  console.log(`✅ Saved ${all.length} SOLD + SURPLUS auctions`);
  console.log("🏁 Done");
})();