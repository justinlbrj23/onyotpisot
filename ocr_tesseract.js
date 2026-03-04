import { execSync } from "child_process";
import fs from "fs";
import path from "path";

// ------------------------------
// Config
// ------------------------------
const inputFile = process.argv[2];
const OUT_DIR = "ocr_output";

// Supported input types
const ALLOWED_IMAGE_EXTS = [".jpeg", ".jpg", ".png"];

// ------------------------------
// Helper: clean output folder
// ------------------------------
if (fs.existsSync(OUT_DIR)) {
  fs.rmSync(OUT_DIR, { recursive: true, force: true });
}
fs.mkdirSync(OUT_DIR, { recursive: true });

// ------------------------------
// Check input file exists
// ------------------------------
if (!inputFile) {
  console.error("❌ Usage: node ocr_tesseract.js <image>");
  process.exit(1);
}

if (!fs.existsSync(inputFile)) {
  console.error(`❌ File not found: ${inputFile}`);
  process.exit(1);
}

const ext = path.extname(inputFile).toLowerCase();

if (!ALLOWED_IMAGE_EXTS.includes(ext)) {
  console.error(`❌ Unsupported file type: ${ext}. Only .jpeg, .jpg, .png allowed.`);
  process.exit(1);
}

// ------------------------------
// Regex entity extraction
// ------------------------------
function extractEntities(text) {
  return {
    amounts: text.match(/\b(?:₱|\$)?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b/g) || [],
    dates: text.match(/\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b/g) || [],
    ids: text.match(/\b[A-Z0-9]{6,}\b/g) || []
  };
}

// ------------------------------
// Image preprocessing
// ------------------------------
function preprocessImage(inputPath, outputPath) {
  execSync(
    `convert "${inputPath}" -colorspace Gray -resize 300% -contrast-stretch 0 "${outputPath}"`
  );
}

// ------------------------------
// OCR pipeline
// ------------------------------
async function runOCR() {
  try {
    // Preprocess input image to improve OCR accuracy
    const cleanPath = `${OUT_DIR}/clean-image.jpeg`;
    preprocessImage(inputFile, cleanPath);

    // Run Tesseract OCR
    let text = "";
    try {
      text = execSync(`tesseract "${cleanPath}" stdout`, { encoding: "utf8" });
    } catch (e) {
      console.warn(`⚠️ Tesseract failed: ${e.message}`);
      text = "";
    }

    // Extract entities
    const entities = extractEntities(text);

    // Save output files
    fs.writeFileSync(`${OUT_DIR}/ocr.txt`, text);
    fs.writeFileSync(`${OUT_DIR}/output.json`, JSON.stringify({ entities, text }, null, 2));

    // Output results to stdout
    console.log("===== OCR TEXT =====");
    console.log(text.trim());
    console.log("===== EXTRACTED ENTITIES =====");
    console.log(JSON.stringify(entities, null, 2));

    // Clean up intermediate file
    fs.rmSync(cleanPath, { force: true });

    console.log("✅ OCR complete (single image, JPEG)");

  } catch (err) {
    console.error("❌ OCR failed:", err.message);
    process.exit(1);
  }
}

runOCR();