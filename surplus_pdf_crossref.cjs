const fs = require("fs");
const { google } = require("googleapis");
const pdfParse = require("pdf-parse");

// ==============================
// CONFIG
// ==============================
const PDF_URL =
  "https://img1.wsimg.com/blobby/go/dcf61f4f-af9a-47b3-9cc6-0b2cf55ca500/downloads/2cc74735-1c8c-4180-8dd3-7fe2d3fa0218/Surplus%20File%20Update%20List.pdf?ver=1773692360706";

const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main";
const SOURCE_RANGE = `${SHEET_NAME}!G2:G`;
const OUTPUT_COLUMN = "AT";
const MATCH_THRESHOLD = 0.80;

// ==============================
// HELPERS
// ==============================
function normalizeText(str = "") {
  return String(str)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")      // remove accents
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")         // keep only alphanumeric + spaces
    .replace(/\s+/g, " ")
    .trim();
}

function getBigrams(str) {
  const s = normalizeText(str).replace(/\s+/g, " ");
  const bigrams = [];
  if (s.length < 2) return bigrams;
  for (let i = 0; i < s.length - 1; i++) {
    bigrams.push(s.slice(i, i + 2));
  }
  return bigrams;
}

function diceCoefficient(a, b) {
  const bgA = getBigrams(a);
  const bgB = getBigrams(b);

  if (!bgA.length || !bgB.length) return 0;

  const map = new Map();
  for (const x of bgA) {
    map.set(x, (map.get(x) || 0) + 1);
  }

  let intersection = 0;
  for (const x of bgB) {
    const count = map.get(x) || 0;
    if (count > 0) {
      map.set(x, count - 1);
      intersection++;
    }
  }

  return (2 * intersection) / (bgA.length + bgB.length);
}

function tokenSetSimilarity(a, b) {
  const setA = new Set(normalizeText(a).split(" ").filter(Boolean));
  const setB = new Set(normalizeText(b).split(" ").filter(Boolean));

  if (!setA.size || !setB.size) return 0;

  let common = 0;
  for (const token of setA) {
    if (setB.has(token)) common++;
  }

  return common / Math.max(setA.size, setB.size);
}

function containsLoose(a, b) {
  const na = normalizeText(a);
  const nb = normalizeText(b);
  return na.includes(nb) || nb.includes(na);
}

function similarityScore(a, b) {
  const na = normalizeText(a);
  const nb = normalizeText(b);

  if (!na || !nb) return 0;

  // Strong direct containment shortcut
  if (containsLoose(na, nb)) return 1.0;

  // Two fuzzy signals
  const dice = diceCoefficient(na, nb);
  const token = tokenSetSimilarity(na, nb);

  // Weighted blend
  return (dice * 0.65) + (token * 0.35);
}

function buildPdfCandidates(pdfText) {
  const rawLines = pdfText
    .split(/\r?\n/)
    .map(line => normalizeText(line))
    .filter(line => line.length > 1);

  const candidates = new Set();

  // Single lines
  for (const line of rawLines) {
    candidates.add(line);
  }

  // 2-line windows (helps if PDF wraps text across lines)
  for (let i = 0; i < rawLines.length - 1; i++) {
    const joined = `${rawLines[i]} ${rawLines[i + 1]}`.trim();
    if (joined) candidates.add(joined);
  }

  // 3-line windows
  for (let i = 0; i < rawLines.length - 2; i++) {
    const joined = `${rawLines[i]} ${rawLines[i + 1]} ${rawLines[i + 2]}`.trim();
    if (joined) candidates.add(joined);
  }

  return Array.from(candidates);
}

async function downloadPdfBuffer(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to download PDF: ${res.status} ${res.statusText}`);
  }
  const arrayBuffer = await res.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "service-account.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

// ==============================
// MAIN
// ==============================
async function main() {
  if (!fs.existsSync("service-account.json")) {
    throw new Error("Missing service-account.json");
  }

  console.log("Downloading PDF...");
  const pdfBuffer = await downloadPdfBuffer(PDF_URL);

  console.log("Parsing PDF...");
  const pdfData = await pdfParse(pdfBuffer);
  const pdfText = pdfData.text || "";
  const normalizedPdfText = normalizeText(pdfText);
  const pdfCandidates = buildPdfCandidates(pdfText);

  console.log(`PDF parsed. Candidates built: ${pdfCandidates.length}`);

  const sheets = await getSheetsClient();

  console.log(`Reading source values from ${SOURCE_RANGE}...`);
  const readRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: SOURCE_RANGE,
    majorDimension: "ROWS",
  });

  const rows = readRes.data.values || [];
  console.log(`Rows fetched from sheet: ${rows.length}`);

  const outputValues = [];

  for (let i = 0; i < rows.length; i++) {
    const originalValue = rows[i]?.[0] || "";
    const rowNumber = i + 2; // because source starts at G2

    if (!originalValue || !String(originalValue).trim()) {
      outputValues.push([""]);
      console.log(`Row ${rowNumber}: EMPTY -> blank`);
      continue;
    }

    const value = normalizeText(originalValue);
    let bestScore = 0;
    let bestCandidate = "";

    // Fast direct whole-PDF containment
    if (normalizedPdfText.includes(value)) {
      bestScore = 1;
      bestCandidate = "[WHOLE PDF MATCH]";
    } else {
      // Compare against candidate lines/windows
      for (const candidate of pdfCandidates) {
        const score = similarityScore(value, candidate);
        if (score > bestScore) {
          bestScore = score;
          bestCandidate = candidate;
        }

        // early exit if perfect
        if (bestScore >= 1) break;
      }
    }

    const verdict = bestScore >= MATCH_THRESHOLD ? "YES" : "NO";
    outputValues.push([verdict]);

    console.log(
      `Row ${rowNumber}: "${originalValue}" -> ${verdict} (score=${bestScore.toFixed(
        3
      )})`
    );

    // Uncomment for debugging best candidate:
    // console.log(`   Best candidate: ${bestCandidate}`);
  }

  const outputEndRow = outputValues.length + 1; // starts at row 2
  const outputRange = `${SHEET_NAME}!${OUTPUT_COLUMN}2:${OUTPUT_COLUMN}${outputEndRow}`;

  console.log(`Writing results to ${outputRange}...`);
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: outputRange,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: outputValues,
    },
  });

  console.log("Done. AT column updated successfully.");
}

main().catch(err => {
  console.error("Script failed:", err);
  process.exit(1);
});
