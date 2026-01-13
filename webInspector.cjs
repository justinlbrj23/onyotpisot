// webInspector.cjs
// Page intelligence + pagination discovery (NO data extraction)
// Puppeteer-version safe (no waitForTimeout)

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

const OUTPUT_FILE = "raw-elements.json";

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
// INSPECT PAGE
// =========================
async function inspect(browser, url) {
  const page = await browser.newPage();

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120"
  );

  console.log(`🌐 Inspecting ${url}`);
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });

  // ⬇️ version-safe delay
  await new Promise(r => setTimeout(r, 5000));

  const result = await page.evaluate(() => {
    const soldBadges = document.querySelectorAll(".AuctionSold");
    const nextBtn =
      document.querySelector('a[title*="Next"]') ||
      document.querySelector('a img[alt="Next"]')?.closest("a");

    return {
      soldFound: soldBadges.length > 0,
      soldCount: soldBadges.length,
      hasNextButton: !!nextBtn,
      paginationType: nextBtn ? "js-click" : "none",
    };
  });

  await page.close();
  return result;
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

  const report = [];

  for (const url of urls) {
    const info = await inspect(browser, url);
    report.push({ url, ...info });
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(report, null, 2));
  console.log(`✅ Saved inspector output → ${OUTPUT_FILE}`);
})();