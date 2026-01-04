const { createWorker } = require("tesseract.js");
const fs = require("fs-extra");
const path = require("path");
const sharp = require("sharp");
const pdf = require("pdf-poppler");

const INPUT_PATH = process.argv[2];
const OUTPUT_FILE = "ocr_output.txt";

if (!INPUT_PATH) {
  console.error("❌ Usage: node ocr_extract.cjs <file.pdf|image.png>");
  process.exit(1);
}

async function convertPdfToImages(pdfPath) {
  const outDir = "./_pdf_pages";
  await fs.ensureDir(outDir);

  await pdf.convert(pdfPath, {
    format: "png",
    out_dir: outDir,
    out_prefix: "page",
    page: null,
  });

  return fs.readdir(outDir)
    .filter(f => f.endsWith(".png"))
    .map(f => path.join(outDir, f));
}

async function preprocessImage(imagePath) {
  const processedPath = imagePath.replace(".png", "_processed.png");
  await sharp(imagePath)
    .grayscale()
    .normalize()
    .threshold(180)
    .toFile(processedPath);
  return processedPath;
}

async function runOCR(images) {
  const worker = await createWorker("eng");
  let fullText = "";

  for (const img of images) {
    const processed = await preprocessImage(img);
    const { data } = await worker.recognize(processed);
    fullText += `\n\n===== ${path.basename(img)} =====\n`;
    fullText += data.text;
  }

  await worker.terminate();
  return fullText;
}

(async () => {
  let images = [];

  if (INPUT_PATH.toLowerCase().endsWith(".pdf")) {
    images = await convertPdfToImages(INPUT_PATH);
  } else {
    images = [INPUT_PATH];
  }

  const text = await runOCR(images);
  await fs.writeFile(OUTPUT_FILE, text, "utf8");

  console.log("✅ OCR complete");
  console.log("📄 Output saved to:", OUTPUT_FILE);
})();