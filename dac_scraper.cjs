/**
 * Dallas CAD Scraper
 * Requirements:
 *  - Reads PARCEL_ID from Google Sheet column F
 *  - Visits two URLs per parcel, screenshots both pages
 *  - Creates a Drive subfolder per parcel, uploads JPEGs
 *  - Extracts owner name from URL 2 and writes back to Sheet column N
 */

import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { google } from "googleapis";
import fs from "fs";
import path from "path";

puppeteer.use(StealthPlugin());

// --------------------------------------------------
// CONFIG
// --------------------------------------------------
const SHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const PARCEL_RANGE = "F2:F";
const OUTPUT_RANGE_COL = "N"; // owner name output column
const DRIVE_PARENT_FOLDER = "11c9BxTj6ej-fJNvECJM_oBDz3WfsSkWl";

const TARGET_URL_1 = "https://www.dallascad.org/AcctDetailRes.aspx?ID=";
const TARGET_URL_2 = "https://www.dallascad.org/AcctHistory.aspx?ID=";

// --------------------------------------------------
// GOOGLE AUTH
// --------------------------------------------------
const auth = new google.auth.GoogleAuth({
  keyFile: "service-account.json",
  scopes: ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"],
});

const drive = google.drive({ version: "v3", auth });
const sheets = google.sheets({ version: "v4", auth });

// --------------------------------------------------
// MAIN FUNCTION
// --------------------------------------------------
async function runScraper() {
  console.log("Reading PARCEL IDs from sheet...");

  const sheetResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: PARCEL_RANGE,
  });

  const parcelIDs = sheetResp.data.values?.flat() || [];
  if (parcelIDs.length === 0) {
    console.log("No PARCEL_IDs found.");
    return;
  }

  console.log(`Found ${parcelIDs.length} parcels.`);

  const browser = await puppeteer.launch({
    headless: "new",
    defaultViewport: null,
  });

  const page = await browser.newPage();

  for (let i = 0; i < parcelIDs.length; i++) {
    const parcel = parcelIDs[i];
    if (!parcel) continue;

    console.log(`Processing Parcel ID: ${parcel}`);

    // ---------------------------------------
    // Create Drive subfolder
    // ---------------------------------------
    const folder = await drive.files.create({
      requestBody: {
        name: `${parcel}`,
        mimeType: "application/vnd.google-apps.folder",
        parents: [DRIVE_PARENT_FOLDER],
      },
      fields: "id",
    });

    const folderId = folder.data.id;
    console.log(`Created Drive folder: ${folderId}`);

    // ---------------------------------------
    // TARGET URL 1 — Screenshot
    // ---------------------------------------
    const url1 = TARGET_URL_1 + parcel;
    await page.goto(url1, { waitUntil: "networkidle2" });

    const file1Path = `screenshot_detail_${parcel}.jpg`;
    await page.screenshot({ path: file1Path, type: "jpeg", fullPage: true });

    await uploadFileToDrive(folderId, file1Path, `DETAIL_${parcel}.jpg`);
    fs.unlinkSync(file1Path);

    console.log("Uploaded screenshot 1");

    // ---------------------------------------
    // TARGET URL 2 — Screenshot + Owner Name Parsing
    // ---------------------------------------
    const url2 = TARGET_URL_2 + parcel;
    await page.goto(url2, { waitUntil: "networkidle2" });

    const file2Path = `screenshot_history_${parcel}.jpg`;
    await page.screenshot({ path: file2Path, type: "jpeg", fullPage: true });

    await uploadFileToDrive(folderId, file2Path, `HISTORY_${parcel}.jpg`);
    fs.unlinkSync(file2Path);

    console.log("Uploaded screenshot 2");

    // ---------------------------------------
    // Extract OWNER NAME
    // Table: second column, often contains "Name + Address"
    // ---------------------------------------
    const ownerName = await page.evaluate(() => {
      const tableCells = Array.from(document.querySelectorAll("table td"));
      if (tableCells.length < 2) return "";

      const rawText = tableCells[1].innerText.trim();

      // extract only FIRST LINE before the address
      return rawText.split("\n")[0].trim();
    });

    console.log("Owner Name:", ownerName);

    // ---------------------------------------
    // Write owner name back to sheet
    // ---------------------------------------
    const writeRange = `${OUTPUT_RANGE_COL}${i + 2}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: writeRange,
      valueInputOption: "RAW",
      requestBody: { values: [[ownerName]] },
    });

    console.log(`Wrote owner name to sheet row ${i + 2}`);
  }

  await browser.close();
  console.log("DONE.");
}

// --------------------------------------------------
// DRIVE UPLOAD HELPERS
// --------------------------------------------------
async function uploadFileToDrive(parentFolderId, localFilePath, newName) {
  const fileMetadata = {
    name: newName,
    parents: [parentFolderId],
  };

  const media = {
    mimeType: "image/jpeg",
    body: fs.createReadStream(localFilePath),
  };

  await drive.files.create({
    requestBody: fileMetadata,
    media: media,
    fields: "id",
  });
}

// --------------------------------------------------
runScraper().catch(console.error);