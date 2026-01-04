const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const inputFile = process.argv[2];

if (!inputFile) {
  console.error("❌ Usage: node ocr_tesseract.cjs <image|pdf>");
  process.exit(1);
}

const ext = path.extname(inputFile).toLowerCase();
const outputBase = "ocr_output";

try {
  if (ext === ".pdf") {
    console.log("📄 PDF detected — converting pages to images...");
    execSync(`pdftoppm "${inputFile}" page -png`, { stdio: "inherit" });

    const pages = fs.readdirSync(".").filter(f => f.startsWith("page-") && f.endsWith(".png"));

    let fullText = "";
    for (const page of pages) {
      console.log(`🔍 OCR: ${page}`);
      const text = execSync(`tesseract "${page}" stdout`, { encoding: "utf8" });
      fullText += `\n\n=== ${page} ===\n\n` + text;
    }

    fs.writeFileSync(`${outputBase}.txt`, fullText);
  } else {
    console.log("🖼 Image detected — running OCR...");
    execSync(`tesseract "${inputFile}" ${outputBase}`, { stdio: "inherit" });
  }

  console.log("✅ OCR complete (Tesseract)");
} catch (err) {
  console.error("❌ OCR failed:", err.message);
  process.exit(1);
}