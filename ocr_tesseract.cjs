const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const inputFile = process.argv[2];

if (!inputFile) {
  console.error("❌ Usage: node ocr_tesseract.cjs <image|pdf>");
  process.exit(1);
}

if (!fs.existsSync(inputFile)) {
  console.error(`❌ File not found: ${inputFile}`);
  process.exit(1);
}

const ext = path.extname(inputFile).toLowerCase();
const OUT_DIR = "ocr_output";
fs.mkdirSync(OUT_DIR, { recursive: true });

/* --------------------------------------------------
   Regex entity extraction (customizable)
-------------------------------------------------- */
function extractEntities(text) {
  return {
    amounts: text.match(/\b(?:₱|\$)?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b/g) || [],
    dates: text.match(/\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b/g) || [],
    ids: text.match(/\b[A-Z0-9]{6,}\b/g) || []
  };
}

/* --------------------------------------------------
   Image preprocessing for better OCR accuracy
-------------------------------------------------- */
function preprocessImage(inputPath, outputPath) {
  execSync(
    `convert "${inputPath}" -colorspace Gray -resize 300% -contrast-stretch 0 "${outputPath}"`
  );
}

try {
  let pageImages = [];

  // --------------------------------------------------
  // PDF → images
  // --------------------------------------------------
  if (ext === ".pdf") {
    console.log("📄 PDF detected — converting pages to images...");
    execSync(`pdftoppm "${inputFile}" ${OUT_DIR}/page -png`);

    pageImages = fs.readdirSync(OUT_DIR)
      .filter(f => f.startsWith("page-") && f.endsWith(".png"))
      .sort((a, b) => {
        const na = parseInt(a.match(/page-(\d+)/)[1], 10);
        const nb = parseInt(b.match(/page-(\d+)/)[1], 10);
        return na - nb;
      });
  } 
  // --------------------------------------------------
  // Image → normalize as page-1
  // --------------------------------------------------
  else {
    const target = `${OUT_DIR}/page-1.png`;
    fs.copyFileSync(inputFile, target);
    pageImages = ["page-1.png"];
  }

  let combinedText = "";
  const jsonResults = [];

  // --------------------------------------------------
  // OCR each page
  // --------------------------------------------------
  for (let i = 0; i < pageImages.length; i++) {
    const pageNum = i + 1;
    const rawPath = `${OUT_DIR}/${pageImages[i]}`;
    const cleanPath = `${OUT_DIR}/clean-page-${pageNum}.png`;

    console.log(`🔍 OCR page ${pageNum}`);

    preprocessImage(rawPath, cleanPath);

    const text = execSync(`tesseract "${cleanPath}" stdout`, {
      encoding: "utf8"
    });

    const entities = extractEntities(text);

    fs.writeFileSync(`${OUT_DIR}/page-${pageNum}.txt`, text);

    jsonResults.push({
      page: pageNum,
      entities,
      text
    });

    combinedText += `\n\n===== PAGE ${pageNum} =====\n\n${text}`;
  }

  // --------------------------------------------------
  // Final outputs
  // --------------------------------------------------
  fs.writeFileSync(`${OUT_DIR}/full_text.txt`, combinedText);
  fs.writeFileSync(
    `${OUT_DIR}/output.json`,
    JSON.stringify(jsonResults, null, 2)
  );

  console.log("✅ OCR complete (Advanced Tesseract)");

} catch (err) {
  console.error("❌ OCR failed:", err.message);
  process.exit(1);
}