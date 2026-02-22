const fs = require("fs");
const puppeteer = require("puppeteer");
const { google } = require("googleapis");

// =========================
// GOOGLE SHEETS CONFIG
// =========================

const SHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "Palm Beach - Taxdeed";
const SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];
const TOKEN_PATH = "token.json";
const CREDENTIALS_PATH = "credentials.json";

// =========================
// GOOGLE AUTH
// =========================

async function authenticateGoogleSheets() {
  const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
  const { client_secret, client_id, redirect_uris } =
    credentials.installed || credentials.web;

  const oAuth2Client = new google.auth.OAuth2(
    client_id,
    client_secret,
    redirect_uris[0]
  );

  if (fs.existsSync(TOKEN_PATH)) {
    oAuth2Client.setCredentials(JSON.parse(fs.readFileSync(TOKEN_PATH)));
  } else {
    const authUrl = oAuth2Client.generateAuthUrl({
      access_type: "offline",
      scope: SCOPES,
    });

    console.log("Authorize this app by visiting this url:", authUrl);

    const readline = require("readline").createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    const code = await new Promise((resolve) => {
      readline.question("Enter code here: ", (code) => {
        readline.close();
        resolve(code);
      });
    });

    const token = await oAuth2Client.getToken(code);
    oAuth2Client.setCredentials(token.tokens);
    fs.writeFileSync(TOKEN_PATH, JSON.stringify(token.tokens));
  }

  return google.sheets({ version: "v4", auth: oAuth2Client });
}

// =========================
// SCRAPER (PUPPETEER)
// =========================

async function scrapeData(url) {
  const browser = await puppeteer.launch({
    headless: false, // set true for headless
    defaultViewport: null,
  });

  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "networkidle2" });

  let results = [];
  let currentPage = 1;
  let totalPages = 1;

  while (true) {
    console.log(`Scraping page ${currentPage}`);

    await page.waitForSelector("div[aid]");

    const pageResults = await page.evaluate(() => {
      const rows = [];

      function getValueByHeader(container, headerText) {
        const ths = Array.from(container.querySelectorAll("th"));
        const th = ths.find(t => t.textContent.includes(headerText));
        if (!th) return "";
        const td = th.nextElementSibling;
        return td ? td.textContent.trim() : "";
      }

      const items = document.querySelectorAll("div[aid]");

      items.forEach(item => {
        rows.push([
          getValueByHeader(item, "Cause Number:"),
          getValueByHeader(item, "Adjudged Value:"),
          getValueByHeader(item, "Est. Min. Bid:"),
          getValueByHeader(item, "Account Number:"),
          getValueByHeader(item, "Property Address:"),
          item.querySelector("tr:nth-of-type(8) td")?.textContent.trim() || "",
          item.querySelector("div.ASTAT_MSGA")?.textContent.trim() || "",
          item.querySelector("div.ASTAT_MSGD")?.textContent.trim() || ""
        ]);
      });

      return rows;
    });

    results.push(...pageResults);

    // Detect total pages
    try {
      const pageText = await page.$eval("span.PageText", el => el.textContent);
      const match = pageText.match(/Page \d+ of (\d+)/);
      if (match) totalPages = parseInt(match[1]);
    } catch {}

    console.log(`Page ${currentPage} of ${totalPages}`);

    if (currentPage >= totalPages) break;

    try {
      await page.click("span.PageRight");
      await page.waitForTimeout(3000);
      currentPage++;
    } catch {
      break;
    }
  }

  await browser.close();
  return results;
}

// =========================
// GOOGLE SHEETS LOGGER
// =========================

async function logDataToGoogleSheets(data) {
  const sheets = await authenticateGoogleSheets();

  const headers = [
    "Case Number",
    "Adjudged Value",
    "Opening Bid",
    "Parcel ID",
    "Street Address",
    "City State Zip",
    "Status",
    "Sold Amount",
  ];

  const existing = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1:H1`,
  });

  if (!existing.data.values) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1:H1`,
      valueInputOption: "RAW",
      requestBody: { values: [headers] },
    });
  }

  const last = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:A`,
  });

  const lastRow = (last.data.values || []).length + 1;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A${lastRow}:H${lastRow + data.length - 1}`,
    valueInputOption: "RAW",
    requestBody: { values: data },
  });

  console.log("Uploaded to Google Sheets");
}

// =========================
// MAIN
// =========================

(async () => {
  const url =
    "https://dallas.texas.sheriffsaleauctions.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/03/2026";

  const data = await scrapeData(url);

  console.log("Total records:", data.length);

  if (data.length > 0) {
    await logDataToGoogleSheets(data);
  } else {
    console.log("No data found");
  }
})();
