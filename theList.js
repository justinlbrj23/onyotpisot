// theList.js
// Robust OCR pipeline for single-line alphanumeric codes and documents.
// Default: single-line, light-on-dark, whitelist A–Z0–9.
// Requires: Tesseract OCR, ImageMagick ("magick" or "convert"), Poppler (pdftoppm), Ghostscript (gs).

import { spawn, execSync } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";

// ------------------------------
// CLI
// ------------------------------
const inputFile = process.argv[2];
if (!inputFile) {
  console.error("❌ Usage: node ocr_pipeline.js <image|pdf>");
  process.exit(1);
}
if (!fs.existsSync(inputFile)) {
  console.error(`❌ File not found: ${inputFile}`);
  process.exit(1);
}

// ------------------------------
// Config (tunable via environment variables)
// ------------------------------
const OUT_DIR = "ocr_output";
const ALLOWED_IMAGE_EXTS = [".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"];
const ALLOWED_PDF_EXTS = [".pdf"];
const ext = path.extname(inputFile).toLowerCase();

const OCR_DEFAULTS = {
  lang: process.env.OCR_LANG || "eng",
  // For single-line codes use psm=7; for general text blocks use psm=6
  psm: process.env.OCR_PSM || "7",
  // LSTM engine
  oem: process.env.OCR_OEM || "1",
  // Restrict charset if you know the domain
  whitelist: process.env.OCR_WHITELIST || "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
  // PDF rasterization DPI
  dpi: parseInt(process.env.OCR_DPI || "350", 10),
  // Concurrency for multi-page jobs
  maxParallel: Math.max(1, Math.min(os.cpus().length, parseInt(process.env.OCR_MAX_PAR || "4", 10))),
  // Polarity: "light-on-dark" (default here), "dark-on-light", or "auto"
  polarity: (process.env.OCR_POLARITY || "light-on-dark").toLowerCase(),
  // Remove strong ruling lines before binarization (true/false)
  removeLines: (process.env.OCR_REMOVE_LINES || "true").toLowerCase() === "true",
  // Expected code length (optional, used by post-filter); leave empty to skip
  codeLen: process.env.OCR_CODE_LEN ? parseInt(process.env.OCR_CODE_LEN, 10) : null,
};

// Resolve ImageMagick CLI: prefer "magick" (Windows/newer) else "convert"
let IM_BIN = process.env.IM_BIN || null;
if (!IM_BIN) {
  try {
    execSync("magick -version", { stdio: "ignore" });
    IM_BIN = "magick";
  } catch {
    IM_BIN = "convert"; // Linux/older installs
  }
}

// ------------------------------
// FS helpers
// ------------------------------
function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function rmDir(dir) {
  if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
}

// ------------------------------
// Small process runner
// ------------------------------
function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { shell: true, ...opts });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", d => (stdout += d.toString()));
    child.stderr?.on("data", d => (stderr += d.toString()));
    child.on("close", code => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${cmd} ${args.join(" ")}\n${stderr}`));
    });
  });
}

// ------------------------------
// PDF validation
// ------------------------------
function isRealPDF(filePath) {
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.alloc(5);
  fs.readSync(fd, buffer, 0, 5, 0);
  fs.closeSync(fd);
  return buffer.toString() === "%PDF-";
}

// ------------------------------
// Simple entity extraction (kept from your version, slightly expanded)
// ------------------------------
function extractEntities(text) {
  return {
    amounts:
      text.match(/\b(?:₱|₫|₹|€|\$)?\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\b/g) || [],
    dates:
      text.match(/\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b/gi) || [],
    ids: text.match(/\b[A-Z0-9]{6,}\b/g) || [],
  };
}

// ------------------------------
// Heuristics: luminance & polarity
// ------------------------------
async function pageMeanLuminance(inputPath) {
  const identify = IM_BIN === "magick" ? `${IM_BIN} identify` : "identify";
  const { stdout } = await run(identify, ['-format', '%[fx:mean]', `"${inputPath}"`]);
  return parseFloat(stdout.trim());
}
async function shouldInvert(inputPath) {
  const p = OCR_DEFAULTS.polarity;
  if (p === "light-on-dark") return true;
  if (p === "dark-on-light") return false;
  // auto: if globally dark page, invert
  const mean = await pageMeanLuminance(inputPath); // 0 (black) .. 1 (white)
  return mean < 0.45;
}

// ------------------------------
// Image preprocessing (polarity-aware + denoise + adaptive binarization)
// Optional line removal for ruled backgrounds
// ------------------------------
async function preprocessImage(inputPath, outputPath) {
  const convert = IM_BIN === "magick" ? `${IM_BIN} convert` : "convert";
  const invert = await shouldInvert(inputPath);

  const baseArgs = [
    `"${inputPath}"`,
    "-colorspace", "Gray",
    "-normalize",
    // Deskew helps a little for slanted single-line codes; skip if you know they're upright
    "-deskew", "40%",
    ...(invert ? ["-negate"] : []),
    // Mild denoise and sharpen to connect strokes
    "-statistic", "median", "1",
    "-adaptive-sharpen", "0x1",
  ];

  // Optional: suppress strong ruling lines before binarization
  const lineOps = OCR_DEFAULTS.removeLines
    ? [
        // Remove horizontal lines
        "(",
          "+clone",
          "-morphology", "close", "line:1x12",
          "-threshold", "60%",
          "-negate",
        ")",
        "-compose", "darken", "-composite",
        // Remove vertical lines
        "(",
          "+clone",
          "-morphology", "close", "line:12x1",
          "-threshold", "60%",
          "-negate",
        ")",
        "-compose", "darken", "-composite",
      ]
    : [];

  const tailArgs = [
    // Adaptive threshold copes with uneven backgrounds
    "-adaptive-threshold", "15x15+10%",
    `"${outputPath}"`
  ];

  await run(convert, [...baseArgs, ...lineOps, ...tailArgs]);
}

// ------------------------------
// Tesseract OCR
// ------------------------------
async function tesseractOCR(imagePath) {
  const args = [
    `"${imagePath}"`,
    "stdout",
    "-l", OCR_DEFAULTS.lang,
    "--oem", String(OCR_DEFAULTS.oem),
    "--psm", String(OCR_DEFAULTS.psm),
    // Light configs for short alphanumeric tokens
    "-c", "load_system_dawg=0",
    "-c", "load_freq_dawg=0",
  ];
  if (OCR_DEFAULTS.whitelist) {
    args.push("-c", `tessedit_char_whitelist=${OCR_DEFAULTS.whitelist}`);
  }
  const { stdout } = await run("tesseract", args);
  return stdout;
}

// ------------------------------
// Post-processing for single-line codes (optional)
// ------------------------------
function pickBestSingleLine(text) {
  // Keep alphanumeric tokens; optionally enforce fixed length
  const re = OCR_DEFAULTS.codeLen
    ? new RegExp(`\\b[A-Z0-9]{${OCR_DEFAULTS.codeLen}}\\b`, "g")
    : /\b[A-Z0-9]{4,16}\b/g;

  const candidates = (text.toUpperCase().match(re) || []);
  if (candidates.length === 0) return text.trim();

  // Prefer the longest, then first occurrence
  candidates.sort((a, b) => b.length - a.length);
  // Gentle ambiguity fix only if whitelist allows and codeLen matches
  let best = candidates[0];

  // Optional disambiguation (context-aware mapping)
  if (OCR_DEFAULTS.codeLen && best.length === OCR_DEFAULTS.codeLen) {
    best = best
      .replace(/O/g, "0")   // O -> 0
      .replace(/I/g, "1")   // I -> 1
      .replace(/S/g, "5");  // S -> 5
  }
  return best;
}

// ------------------------------
// PDF → PNG @ DPI
// ------------------------------
async function pdfToPngPages(pdfPath, outDir, dpi) {
  try {
    await run("pdftoppm", ["-r", String(dpi), `"${pdfPath}"`, `${outDir}/page`, "-png"]);
  } catch (e) {
    // Normalize via Ghostscript if Poppler fails
    const normalized = `${outDir}/normalized.pdf`;
    await run("gs", [
      "-sDEVICE=pdfwrite",
      "-dCompatibilityLevel=1.4",
      "-dNOPAUSE",
      "-dQUIET",
      "-dBATCH",
      `-sOutputFile="${normalized}"`,
      `"${pdfPath}"`,
    ]);
    await run("pdftoppm", ["-r", String(dpi), `"${normalized}"`, `${outDir}/page`, "-png"]);
  }

  const files = fs.readdirSync(outDir)
    .filter(f => /^page-\d+\.png$/.test(f))
    .filter(f => fs.statSync(path.join(outDir, f)).isFile())
    .sort((a, b) => {
      const na = parseInt(a.match(/page-(\d+)/)[1], 10);
      const nb = parseInt(b.match(/page-(\d+)/)[1], 10);
      return na - nb;
    });

  return files;
}

// ------------------------------
// Main OCR pipeline
// ------------------------------
async function runOCR() {
  // Reset output folder
  if (fs.existsSync(OUT_DIR)) fs.rmSync(OUT_DIR, { recursive: true, force: true });
  fs.mkdirSync(OUT_DIR, { recursive: true });

  let pageImages = [];

  if (ALLOWED_PDF_EXTS.includes(ext)) {
    console.log("📄 PDF detected — validating...");
    if (!isRealPDF(inputFile)) {
      throw new Error("File has .pdf extension but is NOT a valid PDF");
    }
    console.log("✅ PDF valid — converting pages to PNG @ DPI", OCR_DEFAULTS.dpi);
    pageImages = await pdfToPngPages(inputFile, OUT_DIR, OCR_DEFAULTS.dpi);
  } else if (ALLOWED_IMAGE_EXTS.includes(ext)) {
    // Single image input
    const target = `${OUT_DIR}/page-1.png`;
    fs.copyFileSync(inputFile, target);
    pageImages = ["page-1.png"];
  } else {
    throw new Error(
      `Unsupported file type: ${ext}. Supported: ${ALLOWED_PDF_EXTS.concat(ALLOWED_IMAGE_EXTS).join(", ")}`
    );
  }

  if (pageImages.length === 0) throw new Error("No pages found to OCR after conversion");
  console.log("🗂️ Files to process:", pageImages);

  const results = [];
  const queue = [...pageImages];

  // Bounded parallelism
  const workers = Array.from({ length: OCR_DEFAULTS.maxParallel }, () => (async () => {
    while (queue.length) {
      const file = queue.shift();
      const pageNum = parseInt(file.match(/page-(\d+)/)[1], 10);
      const rawPath = path.join(OUT_DIR, file);
      const cleanPath = path.join(OUT_DIR, `clean-page-${pageNum}.png`);

      try {
        console.log(`🔍 Preprocess + OCR page ${pageNum}`);
        await preprocessImage(rawPath, cleanPath);

        let text = await tesseractOCR(cleanPath);
        const cleaned = pickBestSingleLine(text);

        fs.writeFileSync(path.join(OUT_DIR, `page-${pageNum}.txt`), text, "utf8");
        fs.writeFileSync(path.join(OUT_DIR, `page-${pageNum}-best.txt`), cleaned + "\n", "utf8");

        const entities = extractEntities(text);
        results.push({ page: pageNum, best: cleaned, entities, raw: text });
      } catch (e) {
        console.warn(`⚠️ Page ${pageNum} failed: ${e.message}`);
        results.push({ page: pageNum, error: e.message });
      }
    }
  })());

  await Promise.all(workers);

  // Sort and write combined outputs
  results.sort((a, b) => a.page - b.page);
  const combinedText = results
    .map(r => `\n\n===== PAGE ${r.page} =====\n\n${r.raw || ""}`)
    .join("");
  const combinedBest = results
    .map(r => `PAGE ${r.page}: ${r.best ?? "(no result)"}`)
    .join("\n");

  fs.writeFileSync(path.join(OUT_DIR, "full_text.txt"), combinedText, "utf8");
  fs.writeFileSync(path.join(OUT_DIR, "best_lines.txt"), combinedBest + "\n", "utf8");
  fs.writeFileSync(path.join(OUT_DIR, "output.json"), JSON.stringify(results, null, 2));

  console.log("✅ OCR complete");
}

// ------------------------------
// Execute
// ------------------------------
runOCR().catch(err => {
  console.error("❌ OCR failed:", err.message || err);
  process.exit(1);
});