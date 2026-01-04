import { execSync } from "child_process";
import fs from "fs";
import path from "path";

// ------------------------------
// Config
// ------------------------------
const inputFile = process.argv[2];
const OUT_DIR = "ocr_output";
fs.mkdirSync(OUT_DIR, { recursive: true });

if (!inputFile) {
  console.error("❌ Usage: node ocr_tesseract.cjs <image|pdf>");
  process.exit(1);
}

if (!fs.existsSync(inputFile)) {
  console.error(`❌ File not found: ${inputFile}`);
  process.exit(1);
}

const ext = path.extname(inputFile).toLowerCase();

// ------------------------------
// Utility: check for real PDF
// ------------------------------
function isRealPDF(filePath) {
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.alloc(5);
  fs.readSync(fd, buffer, 0, 5, 0);
  fs.closeSync(fd);
  return buffer.toString() === "%PDF-";
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
    let pageImages = [];

    if (ext === ".pdf") {
      console.log("📄 PDF detected — validating...");

      if (!isRealPDF(inputFile)) {
        throw new Error("File has .pdf extension but is NOT a valid PDF");
      }

      console.log("✅ PDF valid — converting pages to images...");
      try {
        execSync(`pdftoppm "${inputFile}" ${OUT_DIR}/page -png`);
      } catch {
        console.warn("⚠️ pdftoppm failed — attempting PDF normalization...");
        const normalizedPDF = `${OUT_DIR}/normalized.pdf`;
        execSync(
          `gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dNOPAUSE -dQUIET -dBATCH -sOutputFile="${normalizedPDF}" "${inputFile}"`
        );
        execSync(`pdftoppm "${normalizedPDF}" ${OUT_DIR}/page -png`);
      }

      pageImages = fs.readdirSync(OUT_DIR)
        .filter(f => f.startsWith("page-") && f.endsWith(".png"))
        .sort((a, b) => {
          const na = parseInt(a.match(/page-(\d+)/)[1], 10);
          const nb = parseInt(b.match(/page-(\d+)/)[1], 10);
          return na - nb;
        });

    } else {
      // Single image input
      const target = `${OUT_DIR}/page-1.png`;
      fs.copyFileSync(inputFile, target);
      pageImages = ["page-1.png"];
    }

    let combinedText = "";
    const jsonResults = [];

    for (let i = 0; i < pageImages.length; i++) {
      const pageNum = i + 1;
      const rawPath = `${OUT_DIR}/${pageImages[i]}`;
      const cleanPath = `${OUT_DIR}/clean-page-${pageNum}.png`;

      console.log(`🔍 OCR page ${pageNum}`);
      preprocessImage(rawPath, cleanPath);

      let text = "";
      try {
        text = execSync(`tesseract "${cleanPath}" stdout`, { encoding: "utf8" });
      } catch (e) {
        console.warn(`⚠️ Tesseract failed on page ${pageNum}: ${e.message}`);
        // Optionally: call Google Vision API as fallback here
      }

      const entities = extractEntities(text);
      fs.writeFileSync(`${OUT_DIR}/page-${pageNum}.txt`, text);

      jsonResults.push({ page: pageNum, entities, text });
      combinedText += `\n\n===== PAGE ${pageNum} =====\n\n${text}`;
    }

    fs.writeFileSync(`${OUT_DIR}/full_text.txt`, combinedText);
    fs.writeFileSync(`${OUT_DIR}/output.json`, JSON.stringify(jsonResults, null, 2));

    console.log("✅ OCR complete (Advanced Tesseract + PDF normalization)");

  } catch (err) {
    console.error("❌ OCR failed:", err.message);
    process.exit(1);
  }
}

runOCR();